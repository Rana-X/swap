#!/usr/bin/env node

import fs from 'node:fs';
import fsp from 'node:fs/promises';
import http from 'node:http';
import path from 'node:path';

const rootDir = process.cwd();
const runsDir = path.join(rootDir, 'runs');
const publicDir = path.join(rootDir, 'ui');
const port = Number(process.env.PORT || 3123);

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);

    if (url.pathname === '/api/runs') {
      return await handleRunsApi(res);
    }

    if (url.pathname.startsWith('/runs/')) {
      return await serveFile(res, safeJoin(runsDir, url.pathname.replace('/runs/', '')), true);
    }

    if (url.pathname === '/' || url.pathname === '/index.html') {
      return await serveFile(res, path.join(publicDir, 'index.html'));
    }

    if (url.pathname === '/app.js') {
      return await serveFile(res, path.join(publicDir, 'app.js'));
    }

    if (url.pathname === '/styles.css') {
      return await serveFile(res, path.join(publicDir, 'styles.css'));
    }

    respondJson(res, 404, { error: 'Not found' });
  } catch (error) {
    respondJson(res, 500, {
      error: 'Viewer server error',
      message: error?.message ?? String(error)
    });
  }
});

server.listen(port, () => {
  console.log(`Run viewer available at http://localhost:${port}`);
});

async function handleRunsApi(res) {
  const runs = await readRuns();
  respondJson(res, 200, { runs });
}

async function readRuns() {
  if (!fs.existsSync(runsDir)) {
    return [];
  }

  const entries = await fsp.readdir(runsDir, { withFileTypes: true });
  const runDirs = entries
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .sort((a, b) => b.localeCompare(a));

  const runs = [];
  for (const name of runDirs) {
    const runPath = path.join(runsDir, name);
    const run = await readRun(runPath, name);
    runs.push(run);
  }

  return runs;
}

async function readRun(runPath, name) {
  const preflight = await readJsonIfExists(path.join(runPath, 'preflight.json'));
  const runwayTask = await readJsonIfExists(path.join(runPath, 'runway-task.json'));
  const requestFailed = await readJsonIfExists(path.join(runPath, 'request.failed.json'));
  const taskFailed = await readJsonIfExists(path.join(runPath, 'task.failed.json'));

  const files = {
    preparedImage: await relativeIfExists(path.join(runPath, 'prepared-image.jpg')),
    normalizedImage: await relativeIfExists(path.join(runPath, 'normalized-image.png')),
    preparedVideo: await relativeIfExists(path.join(runPath, 'prepared-video.mp4')),
    outputVideo: await relativeIfExists(path.join(runPath, 'runway-output.mp4')),
    review: await relativeIfExists(path.join(runPath, 'review.md'))
  };

  let status = 'preflight_only';
  if (runwayTask) {
    status = 'succeeded';
  } else if (taskFailed || requestFailed) {
    status = 'failed';
  }

  return {
    id: name,
    createdAt: preflight?.createdAt ?? null,
    status,
    ratio: preflight?.ratio ?? null,
    warnings: preflight?.warnings ?? [],
    sourceVideo: preflight?.sourceVideo ?? null,
    sourceImage: preflight?.sourceImage ?? null,
    preparedVideo: preflight?.preparedVideo ?? null,
    preparedImage: preflight?.preparedImage ?? null,
    preparation: preflight?.preparation ?? null,
    video: preflight?.video ?? null,
    image: preflight?.image ?? null,
    runwayTask,
    requestFailed,
    taskFailed,
    files
  };
}

async function readJsonIfExists(filePath) {
  if (!fs.existsSync(filePath)) {
    return null;
  }

  const content = await fsp.readFile(filePath, 'utf8');
  return JSON.parse(content);
}

async function relativeIfExists(filePath) {
  if (!fs.existsSync(filePath)) {
    return null;
  }

  return `/runs/${path.relative(runsDir, filePath).split(path.sep).join('/')}`;
}

function safeJoin(base, unsafeRelative) {
  const resolved = path.resolve(base, unsafeRelative);
  if (!resolved.startsWith(path.resolve(base) + path.sep) && resolved !== path.resolve(base)) {
    throw new Error('Invalid path');
  }

  return resolved;
}

async function serveFile(res, filePath, allowBinary = false) {
  if (!fs.existsSync(filePath)) {
    respondJson(res, 404, { error: 'File not found' });
    return;
  }

  const contentType = getContentType(filePath);
  res.writeHead(200, { 'Content-Type': contentType });

  if (allowBinary) {
    fs.createReadStream(filePath).pipe(res);
    return;
  }

  const content = await fsp.readFile(filePath);
  res.end(content);
}

function getContentType(filePath) {
  const extension = path.extname(filePath).toLowerCase();
  switch (extension) {
    case '.html':
      return 'text/html; charset=utf-8';
    case '.css':
      return 'text/css; charset=utf-8';
    case '.js':
      return 'application/javascript; charset=utf-8';
    case '.json':
      return 'application/json; charset=utf-8';
    case '.jpg':
    case '.jpeg':
      return 'image/jpeg';
    case '.png':
      return 'image/png';
    case '.mp4':
      return 'video/mp4';
    case '.md':
      return 'text/markdown; charset=utf-8';
    default:
      return 'application/octet-stream';
  }
}

function respondJson(res, statusCode, payload) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(payload));
}
