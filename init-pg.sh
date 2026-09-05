#!/bin/bash
# Install pgvector extension in Postgres
# Note: uses apt-get (Debian/Ubuntu-based images).
# For production, use a pgvector-enabled image or install via:
#   apt-get install postgresql-16-pgvector
#
# Alternative: use the pgvector/pgvector:pg16 image instead of postgres:16
set -e
echo "Installing pgvector..."
# This will fail gracefully if pgvector is not available
# The migration will skip the vector extension if it's not present
apt-get update && apt-get install -y --no-install-recommends postgresql-16-pgvector || \
  echo "pgvector not available - vector search disabled"
exit 0
