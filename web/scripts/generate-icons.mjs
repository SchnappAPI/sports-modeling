// Run once locally or via GitHub Actions to generate PNG icons from the SVG.
// Usage: node web/scripts/generate-icons.mjs
// Requires: npm install sharp (run from web/ directory)
//
// This script is optional — the SVG at /icon.svg works as apple-touch-icon
// on most modern browsers. PNG icons are only needed for full cross-browser
// PWA compliance. If you want to generate them:
//   cd web && npm install sharp && node scripts/generate-icons.mjs

import sharp from 'sharp';
import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const svg = readFileSync(join(__dirname, '../public/icon.svg'));

for (const size of [192, 512]) {
  await sharp(svg)
    .resize(size, size)
    .png()
    .toFile(join(__dirname, `../public/icon-${size}.png`));
  console.log(`Generated icon-${size}.png`);
}

console.log('Done.');
