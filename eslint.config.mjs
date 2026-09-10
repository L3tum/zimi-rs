// Flat config (eslint v9) for the embedded web UI: web/common.js plus the
// per-page scripts (web/index.js, web/search.js, web/settings.js). The web
// UI JS is classic browser scripts (no import/export), hence
// sourceType: 'script'.
//
// The pages load web/common.js first and use its helpers as globals
// (declared below as readonly for the per-page scripts only): this silences
// no-undef in the page scripts; common.js marks the same names as exported
// (`/* exported … */` at its top) so they don't trip no-unused-vars there.
// Keep both lists in sync with the functions the pages actually call.
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
];
