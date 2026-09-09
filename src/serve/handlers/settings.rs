//! Settings and collections handlers: global settings (with auth-aware
//! redaction of topology values), per-ZIM settings, and user-defined
//! ZIM collections.
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::Json;
use serde::Deserialize;
use std::collections::HashMap;

use crate::serve::openapi::ErrorResponse;
use crate::AppState;

use super::{CreatedIdResponse, OkResponse};

// ─── Settings: response DTOs (OpenAPI schemas) ───────────────────────────────

/// `PUT /settings` response: the applied count and any per-key errors.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct SettingsUpdateResponse {
    /// Number of settings updated. Invariant: equals the applied count since
    /// `update()` returns `Err` on commit failure.
    pub updated: usize,
    /// Per-key errors (empty array when none).
    pub errors: serde_json::Value,
    /// Updated settings grouped by category.
    pub settings: serde_json::Value,
}

/// `PUT /settings/zim/{name}` response.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct PutZimSettingsResponse {
    /// Always true.
    pub ok: bool,
    /// ZIM name.
    pub name: String,
}

// ─── Collections: response/request DTOs (OpenAPI schemas) ────────────────────

/// A user-defined collection of ZIMs, serialized with ZIM names (not ids).
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct ListCollectionsResponse {
    /// All collections.
    pub collections: Vec<Collection>,
}

/// One user-defined collection (member ZIM ids resolved to names).
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct Collection {
    /// Collection id.
    pub id: i32,
    /// URL-safe unique slug.
    pub name: String,
    /// Human-readable display name.
    pub label: String,
    /// Names of the member ZIM archives.
    pub zim_names: Vec<String>,
    /// Whether the collection is marked as a favorite.
    pub is_favorite: bool,
    /// RFC3339 creation timestamp.
    pub created_at: String,
    /// RFC3339 last-update timestamp.
    pub updated_at: String,
}

/// `POST /collections` request body.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct CreateCollectionBody {
    /// URL-safe unique slug (e.g. `science-en`).
    pub name: String,
    /// Human-readable display name.
    pub label: String,
    /// Names of the member ZIM archives.
    #[serde(default)]
    pub zim_names: Vec<String>,
    #[serde(default)]
    /// Mark as a favorite (default `false`).
    pub is_favorite: bool,
}

/// `PUT /collections/{id}` request body; omitted fields keep their current values.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct UpdateCollectionBody {
    /// New slug; omitted to keep the current one.
    pub name: Option<String>,
    /// New display name; omitted to keep the current one.
    pub label: Option<String>,
    /// Replace the member set; omitted to keep the current one.
    pub zim_names: Option<Vec<String>>,
    /// Replace the favorite flag; omitted to keep the current one.
    pub is_favorite: Option<bool>,
}

// ─── Settings ─────────────────────────────────────────────────────────────────

/// `GET /settings` — all settings grouped by category; topology values are
/// redacted for unauthenticated callers (S6).
#[utoipa::path(
    get,
    path = "/settings",
    responses(
        (status = 200, description = "All settings grouped by category", body = serde_json::Value),
        (status = 500, description = "Internal error", body = ErrorResponse)
    )
)]
pub async fn get_settings(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    raw_query: axum::extract::RawQuery,
) -> Json<serde_json::Value> {
    // S6: redact topology for unauthenticated callers. `settings_authed`
    // returns false in open mode (mode != "password"), so EVERY open-mode
    // caller is treated as unauthenticated → topology redacted (BUG-10).
    // The old `if mode == "open" { true }` override forced open mode to skip
    // redaction, leaking torrent.url / embedding.endpoint / torrent.username.
    let authorization = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let authenticated = crate::serve::middleware::settings_authed(
        &state,
        "GET",
        authorization,
        raw_query.0.as_deref(),
    )
    .await;
    Json(state.settings.all_grouped_for(authenticated))
}

/// `PUT /settings` — bulk-update settings by key; unauthenticated writes to
/// security-sensitive keys are rejected with 403 before any DB round-trip.
#[utoipa::path(
    put,
    path = "/settings",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Update result", body = SettingsUpdateResponse),
        (status = 400, description = "Invalid input", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse)
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn put_settings(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<SettingsUpdateResponse>, crate::error::Error> {
    let obj = body
        .as_object()
        .ok_or_else(|| crate::error::Error::InvalidInput("expected a JSON object".into()))?;
    let updates: HashMap<String, serde_json::Value> =
        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    // Real auth for the write path: unlike get_settings (public read by
    // design), an open-mode caller is **unauthenticated** here, so writes to
    // security-sensitive keys are rejected rather than redacted. The auth
    // middleware is the authoritative gate for this route; this re-check
    // exists for direct-call robustness and the 403 fast-path below, and
    // shares the settings token cache (M3) so it costs at most one 100k-iter
    // hash per 60s per token.
    let authorization = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let authenticated =
        crate::serve::middleware::settings_authed(&state, "PUT", authorization, None).await;
    // 403 fast path: unauthenticated writes to security-sensitive keys are
    // rejected up front (before any DB round-trip — a DB error would surface
    // as 503, masking the auth decision). SettingsCache::update keeps its own
    // per-key check as a backstop for other callers.
    if !authenticated {
        let rejected: Vec<&String> = updates
            .keys()
            .filter(|k| crate::settings::is_security_sensitive(k.as_str()))
            .collect();
        if !rejected.is_empty() {
            let names = rejected
                .iter()
                .map(|k| k.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(crate::error::Error::Forbidden(format!(
                "{names}: security-sensitive — requires authentication"
            )));
        }
    }
    let errors = state.settings.update(&updates, authenticated).await?;
    // Invariant: SettingsCache::update pushes exactly one error per rejected
    // key (and continues), so updates.len() - errors.len() is the exact
    // applied count.
    let count = updates.len().saturating_sub(errors.len());
    Ok(Json(SettingsUpdateResponse {
        updated: count,
        errors: errors.into(),
        settings: state.settings.all_grouped_for(authenticated),
    }))
}

/// `GET /settings/zim/{name}` — per-ZIM settings (404 if the ZIM is unknown).
#[utoipa::path(
    get,
    path = "/settings/zim/{name}",
    params(
        ("name" = String, Path, description = "ZIM name")
    ),
    responses(
        (status = 200, description = "Per-ZIM settings", body = serde_json::Value),
        (status = 404, description = "ZIM not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse)
    )
)]
pub async fn get_zim_settings(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<serde_json::Value>, crate::error::Error> {
    match state.settings.get_zim_settings(&name).await? {
        Some(settings) => Ok(Json(settings)),
        None => Err(crate::error::Error::NotFound(format!(
            "ZIM '{name}' not found"
        ))),
    }
}

/// `PUT /settings/zim/{name}` — update per-ZIM settings (404 if the ZIM is unknown).
#[utoipa::path(
    put,
    path = "/settings/zim/{name}",
    params(
        ("name" = String, Path, description = "ZIM name")
    ),
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Updated", body = PutZimSettingsResponse),
        (status = 400, description = "Invalid input", body = ErrorResponse),
        (status = 404, description = "ZIM not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse)
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn put_zim_settings(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<PutZimSettingsResponse>, crate::error::Error> {
    // A non-object body (array/scalar/null) is not a settings update —
    // `update_zim_settings` would skip validation and return a fake
    // `ok:true` no-op, so 400 it up front (same style as `put_settings`).
    if body.as_object().is_none() {
        return Err(crate::error::Error::InvalidInput(
            "expected a JSON object".into(),
        ));
    }
    state.settings.update_zim_settings(&name, &body).await?;
    Ok(Json(PutZimSettingsResponse { ok: true, name }))
}

// ─── Collections ────────────────────────────────────────────────────────────

/// Map ZIM names to their DB ids, validating that every name exists.
fn zim_names_to_ids(state: &AppState, names: &[String]) -> crate::error::Result<Vec<i32>> {
    let zims = state.zims.list();
    let by_name: HashMap<&str, i32> = zims
        .iter()
        .filter_map(|z| z.id.map(|id| (z.name.as_str(), id)))
        .collect();
    names
        .iter()
        .map(|n| {
            by_name
                .get(n.as_str())
                .copied()
                .ok_or_else(|| crate::error::Error::InvalidInput(format!("ZIM '{n}' not found")))
        })
        .collect()
}

/// Build the ZIM id → name map once per request (the ZIM list is cloned a
/// single time instead of once per collection row).
fn zim_id_to_name_map(state: &AppState) -> HashMap<i32, String> {
    state
        .zims
        .list()
        .iter()
        .filter_map(|z| z.id.map(|id| (id, z.name.clone())))
        .collect()
}

/// Shape a `collections` row into a `Collection` DTO (resolving ids → names).
/// `id_to_name` is built once by the caller (see `zim_id_to_name_map`).
fn collection_from_row(
    id_to_name: &HashMap<i32, String>,
    r: &crate::db::collections::CollectionRow,
) -> Collection {
    Collection {
        id: r.id,
        name: r.name.clone(),
        label: r.label.clone(),
        zim_names: r
            .zim_ids
            .iter()
            .filter_map(|id| id_to_name.get(id).cloned())
            .collect(),
        is_favorite: r.is_favorite,
        created_at: r.created_at.to_rfc3339(),
        updated_at: r.updated_at.to_rfc3339(),
    }
}

/// `GET /collections` — user collections with ZIM ids resolved to names.
#[utoipa::path(
    get,
    path = "/collections",
    responses(
        (status = 200, description = "User collections with resolved ZIM names", body = ListCollectionsResponse),
        (status = 500, description = "Internal error", body = ErrorResponse)
    )
)]
pub async fn list_collections(
    State(state): State<AppState>,
) -> Result<Json<ListCollectionsResponse>, crate::error::Error> {
    let rows = crate::db::collections::list_collections(&state.db).await?;
    // Build the id → name map once, not once per row.
    let id_to_name = zim_id_to_name_map(&state);
    let collections: Vec<Collection> = rows
        .iter()
        .map(|r| collection_from_row(&id_to_name, r))
        .collect();
    Ok(Json(ListCollectionsResponse { collections }))
}

/// `POST /collections` — create a collection (409 if the slug already exists).
#[utoipa::path(
    post,
    path = "/collections",
    request_body = CreateCollectionBody,
    responses(
        (status = 201, description = "Collection created", body = CreatedIdResponse),
        (status = 400, description = "Unknown ZIM name", body = ErrorResponse),
        (status = 409, description = "Collection name already exists", body = ErrorResponse)
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn create_collection(
    State(state): State<AppState>,
    Json(body): Json<CreateCollectionBody>,
) -> Result<(StatusCode, Json<CreatedIdResponse>), crate::error::Error> {
    let name = body.name.trim().to_string();
    if name.is_empty() || body.label.trim().is_empty() {
        return Err(crate::error::Error::InvalidInput(
            "name and label are required".into(),
        ));
    }
    let zim_ids = zim_names_to_ids(&state, &body.zim_names)?;

    // BUG-9: a duplicate `name` hits the UNIQUE constraint — surfaced as
    // `InsertOutcome::Duplicate` and mapped to a domain-specific 409 instead
    // of the generic constraint-derived message.
    match crate::db::collections::insert_collection(
        &state.db,
        &name,
        &body.label,
        &zim_ids,
        body.is_favorite,
    )
    .await?
    {
        crate::db::collections::InsertOutcome::Inserted(id) => {
            Ok((StatusCode::CREATED, Json(CreatedIdResponse { id })))
        }
        crate::db::collections::InsertOutcome::Duplicate => Err(crate::error::Error::Conflict(
            format!("collection '{name}' already exists"),
        )),
    }
}

/// `PUT /collections/{id}` — partial update of a collection (404 if it does not exist).
#[utoipa::path(
    put,
    path = "/collections/{id}",
    params(
        ("id" = i32, Path, description = "Collection id")
    ),
    request_body = UpdateCollectionBody,
    responses(
        (status = 200, description = "Collection updated", body = OkResponse),
        (status = 400, description = "Unknown ZIM name", body = ErrorResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse),
        (status = 409, description = "Name already in use", body = ErrorResponse)
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn update_collection(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i32>,
    Json(body): Json<UpdateCollectionBody>,
) -> Result<Json<OkResponse>, crate::error::Error> {
    let name = match body.name.as_deref().map(str::trim) {
        Some(n) if !n.is_empty() => Some(n.to_string()),
        Some(_) => {
            return Err(crate::error::Error::InvalidInput(
                "name cannot be empty".into(),
            ))
        }
        None => None,
    };
    // BUG-16a: mirror create's rejection — an empty (whitespace-only) label is
    // invalid; check pre-DB so the bad input 400s without touching the pool.
    if body.label.as_deref().is_some_and(|l| l.trim().is_empty()) {
        return Err(crate::error::Error::InvalidInput(
            "label cannot be empty".into(),
        ));
    }
    let zim_ids = match body.zim_names.as_deref() {
        Some(names) => Some(zim_names_to_ids(&state, names)?),
        None => None,
    };
    let has_changes =
        name.is_some() || body.label.is_some() || zim_ids.is_some() || body.is_favorite.is_some();
    if !has_changes {
        return Err(crate::error::Error::InvalidInput(
            "nothing to update".into(),
        ));
    }

    let fields = crate::db::collections::UpdateFields {
        name,
        label: body.label.clone(),
        zim_ids,
        is_favorite: body.is_favorite,
    };
    match crate::db::collections::update_collection(&state.db, id, &fields).await? {
        crate::db::collections::UpdateOutcome::Updated => Ok(Json(OkResponse { ok: true })),
        crate::db::collections::UpdateOutcome::NotFound => Err(crate::error::Error::NotFound(
            format!("collection {id} not found"),
        )),
    }
}

/// `DELETE /collections/{id}` — delete a collection (404 if it does not exist).
#[utoipa::path(
    delete,
    path = "/collections/{id}",
    params(
        ("id" = i32, Path, description = "Collection id")
    ),
    responses(
        (status = 200, description = "Collection deleted", body = OkResponse),
        (status = 404, description = "Collection not found", body = ErrorResponse)
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn delete_collection(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i32>,
) -> Result<Json<OkResponse>, crate::error::Error> {
    match crate::db::collections::delete_collection(&state.db, id).await? {
        crate::db::collections::DeleteOutcome::Deleted => Ok(Json(OkResponse { ok: true })),
        crate::db::collections::DeleteOutcome::NotFound => Err(crate::error::Error::NotFound(
            format!("collection {id} not found"),
        )),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::db::collections::CollectionRow;

    /// A row with fixed passthrough fields so each test pins one aspect.
    fn row(zim_ids: Vec<i32>) -> CollectionRow {
        CollectionRow {
            id: 7,
            name: "science".into(),
            label: "Science".into(),
            zim_ids,
            is_favorite: true,
            created_at: chrono::DateTime::parse_from_rfc3339("2024-01-02T03:04:05Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
            updated_at: chrono::DateTime::parse_from_rfc3339("2024-06-07T08:09:10Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        }
    }

    #[test]
    fn collection_from_row_resolves_ids_in_row_order_and_passes_fields_through() {
        let map: HashMap<i32, String> = [(10, "a".into()), (20, "b".into()), (30, "c".into())]
            .into_iter()
            .collect();
        // Ids are resolved in the row's `zim_ids` order, not sorted by id.
        let c = collection_from_row(&map, &row(vec![30, 10, 20]));
        assert_eq!(
            c.zim_names,
            vec!["c".to_string(), "a".to_string(), "b".to_string()]
        );
        assert_eq!(c.id, 7);
        assert_eq!(c.name, "science");
        assert_eq!(c.label, "Science");
        assert!(c.is_favorite);
        // RFC3339 rendering of the Utc timestamps (zero sub-seconds → no
        // fractional part).
        assert_eq!(c.created_at, "2024-01-02T03:04:05+00:00");
        assert_eq!(c.updated_at, "2024-06-07T08:09:10+00:00");
    }

    #[test]
    fn collection_from_row_drops_unknown_ids_silently() {
        // Documented behavior: ids missing from the map are dropped (no error,
        // no placeholder) — a collection row can outlive deleted ZIMs.
        let map: HashMap<i32, String> = [(10, "a".into())].into_iter().collect();
        let c = collection_from_row(&map, &row(vec![10, 99, 20]));
        assert_eq!(c.zim_names, vec!["a".to_string()]);
    }

    #[test]
    fn collection_from_row_empty_zim_ids_gives_empty_names() {
        let map: HashMap<i32, String> = [(10, "a".into())].into_iter().collect();
        let c = collection_from_row(&map, &row(Vec::new()));
        assert!(c.zim_names.is_empty());
    }

    #[test]
    fn zim_id_to_name_map_is_empty_when_registry_is_empty() {
        // `test_state_with_settings` builds the in-memory state with a
        // non-existent ZIM dir, so `state.zims.list()` is empty → empty map.
        let state = crate::testing::test_state_with_settings(crate::settings::default_settings());
        assert!(zim_id_to_name_map(&state).is_empty());
        // The populated case is not testable without a live DB: `ZimMeta.id`
        // is only assigned by `persist_to_db` (src/zim/mod.rs) after a
        // successful upsert, so a DB-less `scan()` leaves every ZIM with
        // `id: None`, which `zim_id_to_name_map` filters out.
    }
}
