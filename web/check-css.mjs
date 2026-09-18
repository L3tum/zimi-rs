#!/usr/bin/env node
// CSS syntax check for the embedded web UI, built on css-tree — the
// project's sanctioned CSS parser dependency (mirroring jsdom, the
// sanctioned exception for web-test; it replaced the old hand-rolled
// parser, whose conservative rejects are now the real parser's).
//
// Usage: node web/check-css.mjs <file>...
//   *.css  → the whole file is checked as CSS.
//   *.html → only the contents of its <style>…</style> blocks are checked.
//
// Every parse error is reported as file:line: message and the process
// exits non-zero (exit 2 on missing arguments). One thing the lenient
// css-tree parser does not flag — a block left unclosed at end of input
// (EOF counts as a closing }) — gets an explicit brace-balance pass over
// the spec tokenizer, which is quote- and comment-aware, so no
// hand-rolled scanning. Line numbers for HTML sources are relative to the
// <style> block, as in the old script (each block is parsed on its own).
import { readFileSync } from 'node:fs';
import * as csstree from 'css-tree';

let errorCount = 0;

function fail(file, line, msg) {
  errorCount += 1;
  console.error(`${file}:${line}: ${msg}`);
}

/** Check one CSS source (a whole .css file or one <style> block). */
function checkCss(css, file) {
  // 1) Brace balance: the tokenizer (unlike a regex) already consumes
  //    quotes/comments as whole tokens, so a stray } inside a string or
  //    comment never counts. Unclosed blocks report at the innermost
  //    still-open { (the line the author most likely forgot the }).
  const toLine = (offset) => new csstree.OffsetToLocation(css).getLocation(offset).line;
  const opens = [];
  csstree.tokenize(css, (type, start) => {
    if (type === csstree.tokenTypes.LeftCurlyBracket) opens.push(start);
    else if (type === csstree.tokenTypes.RightCurlyBracket) opens.pop();
  });
  if (opens.length > 0) {
    fail(file, toLine(opens[opens.length - 1]), 'unbalanced { (no matching })');
  }
  // 2) Real parse: every syntax error (selectors, declarations, at-rules,
  //    stray }) is reported with its line; parsing continues past each
  //    error, so ALL of them surface, not just the first.
  csstree.parse(css, {
    onParseError(err) {
      fail(file, err.line, err.message);
    },
  });
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
      checkCss(css, file);
    }
  }
  if (errorCount > 0) {
    console.error(`web/check-css: ${errorCount} error(s) in ${checked} CSS source(s)`);
    process.exit(1);
  }
  console.log(`web/check-css: ${checked} CSS source(s) OK`);
}

main();
