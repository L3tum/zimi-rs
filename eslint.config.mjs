// Flat config (eslint v9). Scope:
//   1. The embedded web UI: web/common.js plus the per-page scripts
//      (web/index.js, web/search.js, web/settings.js). These are classic
//      browser scripts (no import/export) → sourceType: 'script'.
//   2. The web-UI test suite: tests/web/**/*.mjs. These are ESM + node:test
//      modules → sourceType: 'module' with the node globals.
//   3. The CSS check script: web/check-css.mjs — a node CLI (ESM) that
//      parses the embedded CSS/inline styles, not a browser script.
//
// The pages load web/common.js first and use its helpers + timing constants
// as globals (declared below as readonly for the per-page scripts only): this
// silences no-undef in the page scripts; common.js marks the same names as
// exported (`/* exported … */` at its top) so they don't trip no-unused-vars
// there. Keep both lists in sync with the symbols the pages actually call.
import js from '@eslint/js';
import globals from 'globals';

const commonJsGlobals = {
  apiFetch: 'readonly',
  apiJson: 'readonly',
  esc: 'readonly',
  fmtBytes: 'readonly',
  fmtEta: 'readonly',
  fmtNum: 'readonly',
  snippetHtml: 'readonly',
  toast: 'readonly',
  zimControls: 'readonly',
  CATEGORY_SAVE_DEBOUNCE_MS: 'readonly',
  SAVED_MARKER_FADE_MS: 'readonly',
};

export default [
  { ignores: ['node_modules/**'] },
  js.configs.recommended,
  {
    files: ['web/common.js'],
    languageOptions: { ecmaVersion: 2022, sourceType: 'script', globals: globals.browser },
    rules: {
      'no-unused-vars': ['error', { args: 'none' }],
      'max-len': ['error', { code: 100 }],
    },
  },
  {
    files: ['web/index.js', 'web/search.js', 'web/settings.js'],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: 'script',
      globals: { ...globals.browser, ...commonJsGlobals },
    },
    rules: {
      'no-unused-vars': ['error', { args: 'none' }],
      'max-len': ['error', { code: 100 }],
    },
  },
  {
    files: ['web/check-css.mjs'],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: 'module',
      globals: globals.node,
    },
    rules: {
      'max-len': ['error', { code: 100 }],
    },
  },
  {
    files: ['tests/web/**/*.mjs'],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: 'module',
      globals: globals.node,
    },
    rules: {
      // Same 100-col limit as the page scripts: the test harness is project
      // code too, so keep it in the same style lane (the old gap let ~20+
      // over-length lines slip through unlinted).
      'max-len': ['error', { code: 100 }],
    },
  },
];
