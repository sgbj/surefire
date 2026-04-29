import sharp from 'sharp';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, resolve } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const repoRoot = resolve(__dirname, '..', '..');
const svg = readFileSync(resolve(repoRoot, 'docs/src/assets/surefire.svg'), 'utf-8');

const targetSize = 128;
const padding = 6;
const inner = targetSize - padding * 2;
const aspect = 621 / 826;
const innerHeight = inner;
const innerWidth = Math.round(inner * aspect);

const rendered = await sharp(Buffer.from(svg), { density: 600 })
  .resize(innerWidth, innerHeight, { fit: 'contain', background: { r: 0, g: 0, b: 0, alpha: 0 } })
  .png()
  .toBuffer();

await sharp({
  create: {
    width: targetSize,
    height: targetSize,
    channels: 4,
    background: { r: 0, g: 0, b: 0, alpha: 0 },
  },
})
  .composite([{ input: rendered, gravity: 'center' }])
  .png()
  .toFile(resolve(repoRoot, 'icon.png'));

console.log(`Wrote icon.png (${targetSize}x${targetSize}, content ${innerWidth}x${innerHeight})`);
