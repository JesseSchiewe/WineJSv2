#!/usr/bin/env node

import fs from 'node:fs/promises';
import path from 'node:path';

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) continue;

    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith('--')) {
      args[key] = true;
      continue;
    }

    args[key] = next;
    i += 1;
  }
  return args;
}

function asCleanString(value) {
  if (value === null || value === undefined) return null;
  const s = String(value).trim();
  if (!s || s.toLowerCase() === 'undefined' || s.toLowerCase() === 'null') return null;
  return s;
}

function asScore(value, max) {
  const n = Number.parseFloat(String(value ?? '').trim());
  if (!Number.isFinite(n)) return 0;
  if (n < 0) return 0;
  if (n > max) return max;
  return Math.round(n);
}

function asDateISO(value) {
  const s = asCleanString(value);
  if (!s) return null;

  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;

  const mdy = s.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$/);
  if (mdy) {
    const month = mdy[1].padStart(2, '0');
    const day = mdy[2].padStart(2, '0');
    const year = mdy[3];
    return `${year}-${month}-${day}`;
  }

  return null;
}

function asFlavorList(value) {
  if (Array.isArray(value)) {
    return value
      .map(item => asCleanString(item))
      .filter(Boolean);
  }

  const s = asCleanString(value);
  if (!s) return [];

  return s
    .split(',')
    .map(item => asCleanString(item))
    .filter(Boolean);
}

function uniqueStrings(list) {
  const out = [];
  const seen = new Set();
  for (const item of list) {
    const normalized = item.trim();
    if (!normalized) continue;
    const key = normalized.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(normalized);
  }
  return out;
}

function parseNumericNullable(value) {
  const s = asCleanString(value);
  if (!s) return null;
  const n = Number.parseFloat(s);
  return Number.isFinite(n) ? n : null;
}

function parseExportFile(text) {
  const cleaned = text.replace(/^\uFEFF/, '');

  try {
    return JSON.parse(cleaned);
  } catch (err) {
    // Some exports are emitted as: "<user-id>": { ... } (missing outer braces).
    const trimmed = cleaned.trim();
    if (/^"[^"]+"\s*:\s*\{/.test(trimmed)) {
      return JSON.parse(`{${trimmed}}`);
    }
    throw err;
  }
}

function getReviewEntries(parsed) {
  if (!parsed || typeof parsed !== 'object') return [];

  const topEntries = Object.entries(parsed);

  // Direct review map: { "Wine Key": { data: {...} } }
  const looksLikeDirectMap = topEntries.some(([, value]) => value && typeof value === 'object' && (value.data || value.Producer || value.WineName));
  if (looksLikeDirectMap) return topEntries;

  // User container map: { "<user-id>": { "Wine Key": { data: {...} } } }
  if (topEntries.length === 1) {
    const maybeContainer = topEntries[0][1];
    if (maybeContainer && typeof maybeContainer === 'object') {
      return Object.entries(maybeContainer);
    }
  }

  return topEntries;
}

function transformRecord(raw) {
  const source = raw?.data ?? raw;
  if (!source || typeof source !== 'object') return null;

  const producer = asCleanString(source.Producer);
  const wineName = asCleanString(source.WineName);
  if (!producer && !wineName) return null;

  const scores = {
    nose: asScore(source.NoseIntensity, 5),
    flavor: asScore(source.FlavorIntensity, 10),
    chars: asScore(source.FlavorCharacteristics, 25),
    balance: asScore(source.Balance, 5),
    length: asScore(source.Length, 5),
  };

  const flavorList = uniqueStrings([
    ...asFlavorList(source.Flavors),
    ...asFlavorList(source.Aromas),
  ]);

  const sum = scores.nose + scores.flavor + scores.chars + scores.balance + scores.length;
  const totalFromSource = Number.parseFloat(String(source.Total ?? '').trim());
  const total = Number.isFinite(totalFromSource) ? Math.round(totalFromSource) : (sum === 0 ? 0 : 50 + sum);

  return {
    date: asDateISO(source.ReviewDate),
    producer,
    wine_name: wineName,
    appellation: asCleanString(source.Appellation),
    vintage: asCleanString(source.Vintage),
    scores: {
      ...scores,
      selected_flavors: flavorList,
      wine_color: null,
    },
    total,
    actual_price: parseNumericNullable(source.ActualPrice),
    would_pay: parseNumericNullable(source.WineValue),
    tasting_notes: asCleanString(source.TastingNotes),
  };
}

async function insertBatch({ url, serviceKey, rows }) {
  const endpoint = `${url.replace(/\/$/, '')}/rest/v1/reviews`;
  const res = await fetch(endpoint, {
    method: 'POST',
    headers: {
      apikey: serviceKey,
      Authorization: `Bearer ${serviceKey}`,
      'Content-Type': 'application/json',
      Prefer: 'return=minimal',
    },
    body: JSON.stringify(rows),
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Supabase insert failed (${res.status}): ${body}`);
  }
}

async function main() {
  const args = parseArgs(process.argv);

  const filePath = args.file || 'winejs-db-export.json';
  const userId = args['user-id'];
  const url = args.url || process.env.SUPABASE_URL;
  const serviceKey = args['service-key'] || process.env.SUPABASE_SERVICE_ROLE_KEY;
  const dryRun = Boolean(args['dry-run']);
  const batchSize = Math.max(1, Number.parseInt(String(args['batch-size'] || '200'), 10) || 200);

  if (!userId) {
    throw new Error('Missing --user-id <uuid>');
  }
  if (!url && !dryRun) {
    throw new Error('Missing --url <supabase-url> (or SUPABASE_URL env var)');
  }
  if (!serviceKey && !dryRun) {
    throw new Error('Missing --service-key <key> (or SUPABASE_SERVICE_ROLE_KEY env var)');
  }

  const absolutePath = path.resolve(filePath);
  const rawText = await fs.readFile(absolutePath, 'utf8');
  const parsed = parseExportFile(rawText);
  const reviewEntries = getReviewEntries(parsed);

  const transformed = [];
  let skipped = 0;

  for (const [, value] of reviewEntries) {
    const rec = transformRecord(value);
    if (!rec) {
      skipped += 1;
      continue;
    }
    transformed.push({ user_id: userId, ...rec });
  }

  if (dryRun) {
    console.log(`Dry run complete.`);
    console.log(`Rows ready: ${transformed.length}`);
    console.log(`Rows skipped: ${skipped}`);
    console.log('Sample rows:');
    console.log(JSON.stringify(transformed.slice(0, 3), null, 2));
    return;
  }

  let inserted = 0;
  for (let i = 0; i < transformed.length; i += batchSize) {
    const batch = transformed.slice(i, i + batchSize);
    await insertBatch({ url, serviceKey, rows: batch });
    inserted += batch.length;
    console.log(`Inserted ${inserted}/${transformed.length}`);
  }

  console.log('Done.');
  console.log(`Inserted rows: ${inserted}`);
  console.log(`Skipped rows: ${skipped}`);
}

main().catch(err => {
  console.error(err.message || err);
  process.exit(1);
});
