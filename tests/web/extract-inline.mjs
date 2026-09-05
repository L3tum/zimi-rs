// Extract the inline <script> blocks from the embedded web UI pages so a
// real linter (eslint) can check them. Dependency-free (node:fs/node:path
// only) and run from the repo root: `node tests/web/extract-inline.mjs`.
//
// Uses the same regex the Makefile `web-check` target uses, so the linted
// and syntax-checked surfaces stay identical: inline scripts are <script>
// tags WITHOUT a src= attribute.
//
// Each extracted block is written to .web-lint-tmp/<page>.script<NN>.js.
// The tmp dir is wiped at the start of every run so stale files never
// linger. Pages with zero inline scripts are skipped (that's fine).

import {
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import path from 'node:path';
import process from 'node:process';

const root = process.cwd();
const tmpDir = path.join(root, '.web-lint-tmp');
const pages = ['web/index.html', 'web/search.html', 'web/settings.html'];

// Must match Makefile web-check exactly.
const re = /<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/gi;

// Clean stale extractions, then ensure the dir exists.
if (existsSync(tmpDir)) rmSync(tmpDir, { recursive: true });
mkdirSync(tmpDir, { recursive: true });

let total = 0;
for (const page of pages) {
  const html = readFileSync(path.join(root, page), 'utf8');
  const name = path.basename(page, '.html');
  let n = 0;
  let m;
  re.lastIndex = 0;
  while ((m = re.exec(html)) !== null) {
    n += 1;
    const out = path.join(tmpDir, `${name}.script${String(n).padStart(2, '0')}.js`);
    writeFileSync(out, m[1]);
  }
  total += n;
  console.log(`${page}: ${n} inline script(s) extracted`);
}
console.log(`extract-inline: ${total} inline script(s) total -> ${tmpDir}`);
