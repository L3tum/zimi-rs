#!/usr/bin/env node
// Lightweight CSS syntax check for the embedded web UI — no dependencies,
// runs on plain node (so `make web-check` stays npm-install-free).
//
// Usage: node web/check-css.mjs <file>...
//   *.css  → the whole file is checked as CSS.
//   *.html → only the contents of its <style>…</style> blocks are checked.
//
// What it verifies (a real parse, not a regex):
//   - balanced block comments and { } blocks,
//   - every rule is `selector { declarations }` (or a known at-rule:
//     @media/@supports → nested rules, @keyframes → nested rules,
//     @font-face → declarations, @import/@charset → statement),
//   - every declaration is `property: value` with a non-empty,
//     identifier-shaped property and a non-empty value,
//   - no stray `;` or `}` outside a block, no empty rule blocks.
//
// It is deliberately conservative: valid CSS it doesn't understand (e.g.
// unusual at-rules) is reported as an error rather than silently passed,
// so the check can only reject, never bless a broken file.
import { readFileSync } from 'node:fs';

const NESTED_RULE_ATS = new Set(['media', 'supports', 'keyframes']);
const DECL_ATS = new Set(['font-face']);
const STATEMENT_ATS = new Set(['import', 'charset', 'layer', 'namespace', 'use', 'plugin']);

let errorCount = 0;

function fail(file, line, msg) {
  errorCount += 1;
  console.error(`${file}:${line}: ${msg}`);
}

/** Replace block comments (slash-star … star-slash) with spaces (newlines
 *  preserved) so all offsets and line numbers below refer to the original
 *  text. Returns null (after reporting) on an unterminated comment. */
function stripComments(text, file) {
  let out = '';
  let i = 0;
  while (i < text.length) {
    if (text.startsWith('/*', i)) {
      const end = text.indexOf('*/', i + 2);
      if (end === -1) {
        fail(file, lineOf(text, i), 'unterminated /* comment');
        return null;
      }
      for (let j = i; j < end + 2; j++) out += text[j] === '\n' ? '\n' : ' ';
      i = end + 2;
    } else {
      out += text[i];
      i += 1;
    }
  }
  return out;
}

function lineOf(text, idx) {
  let line = 1;
  for (let j = 0; j < idx; j++) if (text[j] === '\n') line += 1;
  return line;
}

/** Split a declaration block on `;` at paren/quote depth 0 (a value may
 *  legally contain `;` inside a string or function argument). */
function splitDecls(block) {
  const parts = [];
  let cur = '';
  let depth = 0;
  let quote = null;
  for (let i = 0; i < block.length; i++) {
    const ch = block[i];
    if (quote) {
      cur += ch;
      if (ch === quote) quote = null;
      continue;
    }
    if (ch === '"' || ch === "'") {
      quote = ch;
      cur += ch;
      continue;
    }
    if (ch === '(') depth += 1;
    if (ch === ')') depth -= 1;
    if (ch === ';' && depth === 0) {
      parts.push(cur);
      cur = '';
    } else {
      cur += ch;
    }
  }
  parts.push(cur);
  return parts;
}

/** Index of the first `:` at paren/quote depth 0, or -1. */
function topLevelColon(s) {
  let depth = 0;
  let quote = null;
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (quote) {
      if (ch === quote) quote = null;
      continue;
    }
    if (ch === '"' || ch === "'") {
      quote = ch;
      continue;
    }
    if (ch === '(') depth += 1;
    if (ch === ')') depth -= 1;
    if (ch === ':' && depth === 0) return i;
  }
  return -1;
}

/** Parse a `{ … }` body containing `property: value;` declarations.
 *  bodyText is the text between the braces; bodyStart its offset in `text`.
 *  Returns the index just past the closing `}`. */
function parseDecls(bodyText, bodyStart, text, file, where) {
  for (const partRaw of splitDecls(bodyText)) {
    const part = partRaw.trim();
    if (part === '') continue; // trailing / empty segment
    const colon = topLevelColon(part);
    if (colon === -1) {
      fail(file, lineOf(text, bodyStart), `${where}: expected 'property: value', got "${truncate(part)}"`);
      continue;
    }
    const prop = part.slice(0, colon).trim();
    const value = part.slice(colon + 1).trim();
    if (!/^(--|-)?[A-Za-z][A-Za-z0-9-]*$/.test(prop)) {
      fail(file, lineOf(text, bodyStart), `${where}: not a CSS property name: "${truncate(prop)}"`);
      continue;
    }
    if (value === '') {
      fail(file, lineOf(text, bodyStart), `${where}: empty value for "${prop}"`);
    }
  }
}

function truncate(s) {
  return s.length > 60 ? `${s.slice(0, 57)}…` : s;
}

/** Parse a `{ … }` body that is a *list of nested rules* (@media,
 *  @supports, @keyframes). */
function parseNestedRules(bodyText, bodyStart, text, file) {
  let i = 0;
  while (i < bodyText.length) {
    if (/\s/.test(bodyText[i])) {
      i += 1;
      continue;
    }
    if (bodyText[i] === '}') {
      fail(file, lineOf(text, bodyStart + i), 'stray } inside nested rule list');
      i += 1;
      continue;
    }
    const open = bodyText.indexOf('{', i);
    if (open === -1) {
      fail(file, lineOf(text, bodyStart + i), `expected '{' after "${truncate(bodyText.slice(i).trim())}"`);
      break;
    }
    const selector = bodyText.slice(i, open).trim();
    if (selector === '') {
      fail(file, lineOf(text, bodyStart + i), 'empty selector before {');
    }
    const closeText = findMatchingBrace(text, bodyStart + open, file);
    parseDecls(
      bodyText.slice(open + 1, closeText - bodyStart - 1),
      bodyStart + open + 1,
      text,
      file,
      `rule "${selector}"`
    );
    i = closeText - bodyStart;
  }
}

/** Given `text` and the offset of an opening `{`, return the offset just
 *  past its matching `}` (quote-aware so a stray "}" inside a string value
 *  can't confuse it). */
function findMatchingBrace(text, openIdx, file) {
  let depth = 1;
  let quote = null;
  for (let i = openIdx + 1; i < text.length; i++) {
    const ch = text[i];
    if (quote) {
      if (ch === quote) quote = null;
      continue;
    }
    if (ch === '"' || ch === "'") {
      quote = ch;
      continue;
    }
    if (ch === '{') depth += 1;
    if (ch === '}') {
      depth -= 1;
      if (depth === 0) return i + 1;
    }
  }
  fail(file, lineOf(text, openIdx), 'unbalanced { (no matching })');
  return text.length;
}

/** Parse a complete CSS stylesheet (comments already stripped). */
function parseCss(text, file) {
  let i = 0;
  while (i < text.length) {
    if (/\s/.test(text[i])) {
      i += 1;
      continue;
    }
    // Find the end of the prelude: '{', ';', or '}'.
    let j = i;
    let depth = 0;
    let quote = null;
    while (j < text.length) {
      const ch = text[j];
      if (quote) {
        if (ch === quote) quote = null;
        j += 1;
        continue;
      }
      if (ch === '"' || ch === "'") {
        quote = ch;
        j += 1;
        continue;
      }
      if (ch === '(') depth += 1;
      if (ch === ')') depth -= 1;
      if (depth === 0 && (ch === '{' || ch === ';' || ch === '}')) break;
      j += 1;
    }
    const prelude = text.slice(i, j).trim();
    if (j >= text.length) {
      if (prelude !== '') fail(file, lineOf(text, i), `unexpected end of stylesheet after "${truncate(prelude)}"`);
      break;
    }
    const ch = text[j];
    if (ch === ';') {
      if (prelude.startsWith('@') && STATEMENT_ATS.has(prelude.slice(1).split(/\s/, 1)[0].trim())) {
        // @import / @charset / … — statement form, fine.
      } else {
        fail(file, lineOf(text, i), `statement with no block: "${truncate(prelude)};"`);
      }
      i = j + 1;
      continue;
    }
    if (ch === '}') {
      fail(file, lineOf(text, i), 'stray } at top level');
      i = j + 1;
      continue;
    }
    // ch === '{'
    if (prelude === '') {
      fail(file, lineOf(text, i), 'empty selector before {');
    }
    const close = findMatchingBrace(text, j, file);
    const body = text.slice(j + 1, close - 1);
    if (prelude.startsWith('@')) {
      const atName = prelude.slice(1).split(/[\s(]/, 1)[0].trim();
      if (NESTED_RULE_ATS.has(atName)) {
        if (body.trim() === '') fail(file, lineOf(text, j), `empty ${prelude.split(/\s/, 1)[0]} block`);
        parseNestedRules(body, j + 1, text, file);
      } else if (DECL_ATS.has(atName)) {
        if (body.trim() === '') fail(file, lineOf(text, j), `empty ${prelude.split(/\s/, 1)[0]} block`);
        parseDecls(body, j + 1, text, file, prelude.split(/\s/, 1)[0]);
      } else if (STATEMENT_ATS.has(atName)) {
        fail(file, lineOf(text, i), `${prelude.split(/\s/, 1)[0]} takes a statement, not a block`);
      } else {
        fail(file, lineOf(text, i), `unsupported at-rule "${atName}" (extend check-css.mjs if it is valid CSS)`);
      }
    } else {
      if (body.trim() === '') fail(file, lineOf(text, i), `empty rule for "${truncate(prelude)}"`);
      parseDecls(body, j + 1, text, file, `rule "${truncate(prelude)}"`);
    }
    i = close;
  }
}

function extractStyleBlocks(html) {
  const blocks = [];
  const re = /<style[^>]*>([\s\S]*?)<\/style>/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    blocks.push(m[1]);
  }
  return blocks;
}

function main() {
  const files = process.argv.slice(2);
  if (files.length === 0) {
    console.error('usage: node web/check-css.mjs <file.css|file.html>…');
    process.exit(2);
  }
  let checked = 0;
  for (const file of files) {
    const raw = readFileSync(file, 'utf8');
    const sources = file.toLowerCase().endsWith('.html')
      ? extractStyleBlocks(raw)
      : [raw];
    for (const css of sources) {
      checked += 1;
      const text = stripComments(css, file);
      if (text !== null) parseCss(text, file);
    }
  }
  if (errorCount > 0) {
    console.error(`web/check-css: ${errorCount} error(s) in ${checked} CSS source(s)`);
    process.exit(1);
  }
  console.log(`web/check-css: ${checked} CSS source(s) OK`);
}

main();
