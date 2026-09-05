// Flat config (eslint v9) for the embedded web UI: web/common.js plus the
// inline <script> blocks extracted by tests/web/extract-inline.mjs into
// .web-lint-tmp/. The web UI JS is classic browser scripts (no
// import/export), hence sourceType: 'script'.

import js from '@eslint/js';
import globals from 'globals';

export default [
  { ignores: ['node_modules/**', '.web-lint-tmp/node_modules/**'] },
  js.configs.recommended,
  {
    files: ['web/**/*.js', '.web-lint-tmp/**/*.js'],
    languageOptions: { ecmaVersion: 2022, sourceType: 'script', globals: globals.browser },
    rules: { 'no-unused-vars': ['error', { args: 'none' }] },
  },
];
