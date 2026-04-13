#!/usr/bin/env node

import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import { parseArgs } from 'node:util';
import { spawn } from 'node:child_process';

import RunwayML, { PermissionDeniedError, TaskFailedError, TaskTimedOutError } from '@runwayml/sdk';

loadLocalEnv();

const args = parseArgs({
  options: {
    video: { type: 'string' },
    image: { type: 'string' },
    outputDir: { type: 'string' },
    label: { type: 'string' },
    start: { type: 'string' },
    duration: { type: 'string' },
    ratio: { type: 'string' },
    timeoutMinutes: { type: 'string', default: '15' },
    preflightOnly: { type: 'boolean', default: false },
    noAutoCrop: { type: 'boolean', default: false },
    noAutoTrim: { type: 'boolean', default: false },
    help: { type: 'boolean', short: 'h', default: false }
  }
});

if (args.values.help) {
  printHelp();
  process.exit(0);
}

await main(args.values);

async function main(values) {
  const videoPath = requireAbsolute(values.video, '--video');
  const imagePath = requireAbsolute(values.image, '--image');

  assertExists(videoPath, 'video');
  assertExists(imagePath, 'image');

  const outputDir = values.outputDir
    ? path.resolve(values.outputDir)
    : path.join(process.cwd(), 'runs', buildRunSlug(values.label));

  await fsp.mkdir(outputDir, { recursive: true });

  const sourceVideoProbe = await probeVideo(videoPath);
  const sourceImageProbe = await probeImage(imagePath);

  const requestedStart = parseOptionalNumber(values.start, '--start');
  const requestedDuration = parseOptionalNumber(values.duration, '--duration');
  const timeoutMinutes = parseOptionalNumber(values.timeoutMinutes, '--timeoutMinutes');

  const preparedImage = await prepareImage({
    imagePath,
    outputDir,
    disableAutoCrop: values.noAutoCrop
  });

  const preparedVideo = await prepareVideo({
    videoPath,
    outputDir,
    originalDuration: sourceVideoProbe.duration,
    requestedStart,
    requestedDuration,
    disableAutoTrim: values.noAutoTrim
  });

  const preparedImageProbe = await probeImage(preparedImage.path);
  const preparedVideoProbe = await probeVideo(preparedVideo.path);
  const ratio = resolveRatio(values.ratio, preparedVideoProbe.width, preparedVideoProbe.height);
  const warnings = buildWarnings({ imageProbe: preparedImageProbe, videoProbe: preparedVideoProbe });

  const metadata = {
    createdAt: new Date().toISOString(),
    sourceVideo: videoPath,
    sourceImage: imagePath,
    preparedImage: preparedImage.path,
    preparedVideo: preparedVideo.path,
    preflightOnly: values.preflightOnly,
    runwayModel: 'act_two',
    ratio,
    preparation: {
      autoCropApplied: preparedImage.autoCropApplied,
      autoTrimApplied: preparedVideo.autoTrimApplied,
      normalizedImageInput: preparedImage.normalizedInputPath,
      trimStartSeconds: preparedVideo.start,
      trimDurationSeconds: preparedVideo.duration
    },
    image: {
      source: sourceImageProbe,
      prepared: preparedImageProbe
    },
    video: {
      source: sourceVideoProbe,
      prepared: preparedVideoProbe
    },
    warnings
  };

  await writeJson(path.join(outputDir, 'preflight.json'), metadata);

  if (values.preflightOnly) {
    printPreflight(metadata);
    await writeReviewTemplate({ outputDir, metadata, runway: null });
    return;
  }

  const apiKey = process.env.RUNWAYML_API_SECRET || process.env.RUNWAY_API_KEY;
  if (!apiKey) {
    throw new Error('Set RUNWAYML_API_SECRET before running the Runway generation.');
  }

  process.env.RUNWAYML_API_SECRET = apiKey;

  const client = new RunwayML();
  let taskId = null;
  let completedTask;

  try {
    const imageUri = await uploadEphemeral(client, preparedImage.path);
    const videoUri = await uploadEphemeral(client, preparedVideo.path);

    const taskPromise = client.characterPerformance.create({
      model: 'act_two',
      character: {
        type: 'image',
        uri: imageUri
      },
      reference: {
        type: 'video',
        uri: videoUri
      },
      ratio
    });

    taskId = (await taskPromise).id;
    console.log(`Runway task created: ${taskId}`);

    completedTask = await taskPromise.waitForTaskOutput({
      timeout: timeoutMinutes * 60 * 1000
    });
  } catch (error) {
    if (error instanceof TaskFailedError) {
      await writeJson(path.join(outputDir, 'task.failed.json'), error.taskDetails);
      throw new Error(`Runway task failed. Details saved to ${path.join(outputDir, 'task.failed.json')}`);
    }

    if (error instanceof TaskTimedOutError) {
      throw new Error(`Runway task timed out after ${values.timeoutMinutes} minutes. Task id: ${taskId}`);
    }

    if (error instanceof PermissionDeniedError) {
      await writeJson(path.join(outputDir, 'request.failed.json'), serializeError(error));
      throw new Error(`Runway request was denied. Details saved to ${path.join(outputDir, 'request.failed.json')}`);
    }

    await writeJson(path.join(outputDir, 'request.failed.json'), serializeError(error));
    throw new Error(`Runway request failed. Details saved to ${path.join(outputDir, 'request.failed.json')}`);
  }

  const outputUrl = normalizeOutputUrl(completedTask);
  if (!outputUrl) {
    throw new Error('Runway completed the task but no output URL was returned.');
  }

  const downloadedOutput = path.join(outputDir, 'runway-output.mp4');
  await downloadFile(outputUrl, downloadedOutput);

  const runwayMetadata = {
    taskId,
    outputUrl,
    completedTask
  };

  await writeJson(path.join(outputDir, 'runway-task.json'), runwayMetadata);
  await writeReviewTemplate({ outputDir, metadata, runway: runwayMetadata });

  console.log(`Output saved to ${downloadedOutput}`);
  console.log(`Review notes template saved to ${path.join(outputDir, 'review.md')}`);
}

function loadLocalEnv() {
  const envPath = path.join(process.cwd(), '.env');
  if (!fs.existsSync(envPath)) {
    return;
  }

  const content = fs.readFileSync(envPath, 'utf8');
  for (const rawLine of content.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) {
      continue;
    }

    const separatorIndex = line.indexOf('=');
    if (separatorIndex === -1) {
      continue;
    }

    const key = line.slice(0, separatorIndex).trim();
    const value = line.slice(separatorIndex + 1).trim().replace(/^['"]|['"]$/g, '');
    if (!key || process.env[key] !== undefined) {
      continue;
    }

    process.env[key] = value;
  }
}

function printHelp() {
  console.log(`
Usage:
  node scripts/runway-act-two-first-test.mjs --video /abs/video.mp4 --image /abs/image.heic [options]

Options:
  --video             Absolute path to the source performance video
  --image             Absolute path to the target character image
  --outputDir         Directory to write run artifacts into
  --label             Optional suffix for the generated run directory name
  --start             Optional trim start time in seconds
  --duration          Optional trim duration in seconds
  --ratio             Runway output ratio override
  --timeoutMinutes    Polling timeout for Runway generation, default 15
  --preflightOnly     Validate inputs, prepare assets, and stop before calling Runway
  --no-auto-crop      Skip the default tighter portrait crop
  --no-auto-trim      Skip the default 5 second excerpt for long source clips
  -h, --help          Show this help
`.trim());
}

function requireAbsolute(value, flagName) {
  if (!value) {
    throw new Error(`${flagName} is required.`);
  }

  return path.resolve(value);
}

function assertExists(filePath, label) {
  if (!fs.existsSync(filePath)) {
    throw new Error(`The ${label} file does not exist: ${filePath}`);
  }
}

function parseOptionalNumber(value, flagName) {
  if (value === undefined) {
    return null;
  }

  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`${flagName} must be a non-negative number.`);
  }

  return parsed;
}

async function prepareImage({ imagePath, outputDir, disableAutoCrop }) {
  const extension = path.extname(imagePath).toLowerCase();
  let normalizedInputPath = imagePath;

  if (extension === '.heic' || extension === '.heif') {
    normalizedInputPath = await normalizeHeicImage(imagePath, outputDir);
  }

  const normalizedProbe = await probeImage(normalizedInputPath);
  const preparedImagePath = path.join(outputDir, 'prepared-image.jpg');

  if (disableAutoCrop) {
    await reencodeImage(normalizedInputPath, preparedImagePath);
    return {
      path: preparedImagePath,
      normalizedInputPath,
      autoCropApplied: false
    };
  }

  const crop = computePortraitCrop(normalizedProbe.width, normalizedProbe.height);
  await cropImage({
    inputPath: normalizedInputPath,
    outputPath: preparedImagePath,
    crop
  });

  return {
    path: preparedImagePath,
    normalizedInputPath,
    autoCropApplied: true
  };
}

async function normalizeHeicImage(imagePath, outputDir) {
  const qlOutputDir = path.join(outputDir, 'ql');
  await fsp.mkdir(qlOutputDir, { recursive: true });

  await runCommand('qlmanage', [
    '-t',
    '-s',
    '2400',
    '-o',
    qlOutputDir,
    imagePath
  ]);

  const generatedPreview = path.join(qlOutputDir, `${path.basename(imagePath)}.png`);
  if (!fs.existsSync(generatedPreview)) {
    throw new Error(`Quick Look did not produce a normalized preview for ${imagePath}`);
  }

  const normalizedPath = path.join(outputDir, 'normalized-image.png');
  await fsp.copyFile(generatedPreview, normalizedPath);
  return normalizedPath;
}

async function cropImage({ inputPath, outputPath, crop }) {
  const cropFilter = `crop=${crop.width}:${crop.height}:${crop.x}:${crop.y}`;
  await runCommand('ffmpeg', [
    '-y',
    '-i',
    inputPath,
    '-vf',
    cropFilter,
    '-frames:v',
    '1',
    '-update',
    '1',
    outputPath
  ]);
}

async function reencodeImage(inputPath, outputPath) {
  await runCommand('ffmpeg', [
    '-y',
    '-i',
    inputPath,
    '-frames:v',
    '1',
    '-update',
    '1',
    outputPath
  ]);
}

function computePortraitCrop(width, height) {
  const targetAspect = 4 / 5;
  const preferredWidth = Math.round(width * 0.74);
  const maxWidthFromHeight = Math.round(height * targetAspect);
  const cropWidth = clamp(Math.min(preferredWidth, maxWidthFromHeight, width), 1, width);
  const cropHeight = clamp(Math.round(cropWidth / targetAspect), 1, height);
  const x = clamp(Math.round((width - cropWidth) / 2), 0, width - cropWidth);
  const availableTopMargin = Math.max(0, height - cropHeight);
  const y = clamp(Math.round(availableTopMargin * 0.18), 0, height - cropHeight);

  return { width: cropWidth, height: cropHeight, x, y };
}

async function prepareVideo({ videoPath, outputDir, originalDuration, requestedStart, requestedDuration, disableAutoTrim }) {
  let start = requestedStart ?? 0;
  let duration = requestedDuration;
  let autoTrimApplied = false;

  if (requestedStart === null && requestedDuration === null && !disableAutoTrim && originalDuration > 6) {
    duration = 5;
    start = Math.max(0, (originalDuration - duration) / 2);
    autoTrimApplied = true;
  }

  const effectiveDuration = duration ?? Math.max(0, originalDuration - start);
  if (effectiveDuration <= 0) {
    throw new Error('Prepared video duration must be greater than zero.');
  }

  const preparedVideoPath = path.join(outputDir, 'prepared-video.mp4');

  await runCommand('ffmpeg', [
    '-y',
    '-ss',
    String(start),
    '-i',
    videoPath,
    '-t',
    String(effectiveDuration),
    '-map',
    '0:v:0',
    '-map',
    '0:a?',
    '-c:v',
    'libx264',
    '-preset',
    'medium',
    '-crf',
    '18',
    '-c:a',
    'aac',
    '-movflags',
    '+faststart',
    preparedVideoPath
  ]);

  return {
    path: preparedVideoPath,
    start,
    duration: effectiveDuration,
    autoTrimApplied
  };
}

async function probeVideo(videoPath) {
  const raw = await runCommand('ffprobe', [
    '-v',
    'error',
    '-select_streams',
    'v:0',
    '-show_entries',
    'stream=width,height,r_frame_rate,avg_frame_rate,codec_name:format=duration,size',
    '-of',
    'json',
    videoPath
  ]);

  const parsed = JSON.parse(raw);
  const stream = parsed.streams?.[0];
  const format = parsed.format ?? {};

  if (!stream) {
    throw new Error(`No video stream found in ${videoPath}`);
  }

  return {
    path: videoPath,
    width: Number(stream.width),
    height: Number(stream.height),
    codec: stream.codec_name,
    duration: Number(format.duration),
    sizeBytes: Number(format.size),
    frameRate: pickFrameRate(stream.avg_frame_rate || stream.r_frame_rate)
  };
}

async function probeImage(imagePath) {
  const raw = await runCommand('sips', [
    '-g',
    'pixelWidth',
    '-g',
    'pixelHeight',
    '-g',
    'format',
    imagePath
  ]);

  const width = Number(raw.match(/pixelWidth:\s+(\d+)/)?.[1]);
  const height = Number(raw.match(/pixelHeight:\s+(\d+)/)?.[1]);
  const format = raw.match(/format:\s+(.+)/)?.[1]?.trim() ?? 'unknown';
  const stats = await fsp.stat(imagePath);

  return {
    path: imagePath,
    width,
    height,
    format,
    sizeBytes: stats.size
  };
}

function buildWarnings({ imageProbe, videoProbe }) {
  const warnings = [];

  if (videoProbe.duration < 3 || videoProbe.duration > 30) {
    warnings.push(`Reference video should be between 3 and 30 seconds. Current duration: ${videoProbe.duration.toFixed(2)}s.`);
  }

  if (videoProbe.height < videoProbe.width) {
    warnings.push('Reference video is landscape. A vertical talking-head clip usually works better for this test.');
  }

  if (imageProbe.width < 900 || imageProbe.height < 1100) {
    warnings.push('Prepared image is relatively small. A larger portrait crop would usually transfer better.');
  }

  if (imageProbe.width / imageProbe.height > 0.9) {
    warnings.push('Prepared image is still wide. A tighter portrait crop often works better for Act-Two.');
  }

  return warnings;
}

async function uploadEphemeral(client, filePath) {
  const uploaded = await client.uploads.createEphemeral({
    file: fs.createReadStream(filePath)
  });
  const uri = typeof uploaded === 'string' ? uploaded : uploaded?.uri ?? uploaded?.runwayUri;

  if (!uri) {
    throw new Error(`Upload succeeded but no Runway URI was returned for ${filePath}`);
  }

  return uri;
}

function normalizeOutputUrl(task) {
  if (!task) {
    return null;
  }

  if (Array.isArray(task.output) && typeof task.output[0] === 'string') {
    return task.output[0];
  }

  if (Array.isArray(task.output) && task.output[0]?.url) {
    return task.output[0].url;
  }

  if (typeof task.output === 'string') {
    return task.output;
  }

  return null;
}

function resolveRatio(explicitRatio, width, height) {
  const supportedRatios = [
    '1280:720',
    '720:1280',
    '960:960',
    '1104:832',
    '832:1104',
    '1584:672'
  ];

  if (explicitRatio) {
    if (!supportedRatios.includes(explicitRatio)) {
      throw new Error(`--ratio must be one of: ${supportedRatios.join(', ')}`);
    }

    return explicitRatio;
  }

  const targetAspect = width / height;
  let bestRatio = supportedRatios[0];
  let bestScore = Number.POSITIVE_INFINITY;

  for (const candidate of supportedRatios) {
    const [candidateWidth, candidateHeight] = candidate.split(':').map(Number);
    const candidateAspect = candidateWidth / candidateHeight;
    const score = Math.abs(Math.log(targetAspect / candidateAspect));

    if (score < bestScore) {
      bestRatio = candidate;
      bestScore = score;
    }
  }

  return bestRatio;
}

async function downloadFile(url, destinationPath) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to download Runway output: ${response.status} ${response.statusText}`);
  }

  const arrayBuffer = await response.arrayBuffer();
  await fsp.writeFile(destinationPath, Buffer.from(arrayBuffer));
}

async function writeReviewTemplate({ outputDir, metadata, runway }) {
  const reviewPath = path.join(outputDir, 'review.md');
  const runwaySection = runway
    ? `- Task id: \`${runway.taskId}\`\n- Output URL: ${runway.outputUrl}\n`
    : '- Runway was not called in this run.\n';

  const content = `# Review Checklist

## Inputs
- Video: \`${metadata.sourceVideo}\`
- Image: \`${metadata.sourceImage}\`
- Prepared image: \`${metadata.preparedImage}\`
- Prepared video: \`${metadata.preparedVideo}\`
- Ratio: \`${metadata.ratio}\`
- Auto crop: \`${metadata.preparation.autoCropApplied}\`
- Auto trim: \`${metadata.preparation.autoTrimApplied}\`

## Runway
${runwaySection}
## Acceptance checks
- [ ] Identity stays stable across the clip
- [ ] Blinking and expressions feel natural
- [ ] Mouth region stays coherent enough for a later lip-sync/voice stage
- [ ] Result does not immediately read as a pasted face
- [ ] Clip looks believable at normal phone viewing size

## Failure notes
- Mouth issue:
- Identity drift:
- Angle issue:
- Source image issue:
- Source video issue:

## Next step
- [ ] Keep current image and current take
- [ ] Retry with shorter clip
- [ ] Retry with manual timing override
- [ ] Retry with a better portrait image
`;

  await fsp.writeFile(reviewPath, content, 'utf8');
}

function printPreflight(metadata) {
  console.log('Preflight complete.');
  console.log(`Source video: ${metadata.video.source.width}x${metadata.video.source.height}, ${metadata.video.source.duration.toFixed(2)}s, ${metadata.video.source.frameRate ?? 'unknown'} fps`);
  console.log(`Prepared video: ${metadata.video.prepared.width}x${metadata.video.prepared.height}, ${metadata.video.prepared.duration.toFixed(2)}s`);
  console.log(`Source image: ${metadata.image.source.width}x${metadata.image.source.height}, ${metadata.image.source.format}`);
  console.log(`Prepared image: ${metadata.image.prepared.width}x${metadata.image.prepared.height}, ${metadata.image.prepared.format}`);
  console.log(`Ratio: ${metadata.ratio}`);
  console.log(`Auto crop: ${metadata.preparation.autoCropApplied}`);
  console.log(`Auto trim: ${metadata.preparation.autoTrimApplied}`);

  if (metadata.warnings.length === 0) {
    console.log('Warnings: none');
    return;
  }

  console.log('Warnings:');
  for (const warning of metadata.warnings) {
    console.log(`- ${warning}`);
  }
}

function buildRunSlug(label) {
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  if (!label) {
    return stamp;
  }

  const safeLabel = label.toLowerCase().replace(/[^a-z0-9_-]+/g, '-').replace(/^-+|-+$/g, '');
  return safeLabel ? `${stamp}-${safeLabel}` : stamp;
}

async function writeJson(destination, value) {
  await fsp.writeFile(destination, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function pickFrameRate(raw) {
  if (!raw || raw === '0/0') {
    return null;
  }

  const [numerator, denominator] = raw.split('/').map(Number);
  if (!Number.isFinite(numerator) || !Number.isFinite(denominator) || denominator === 0) {
    return null;
  }

  return Number((numerator / denominator).toFixed(3));
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function serializeError(error) {
  return {
    name: error?.name ?? 'Error',
    message: error?.message ?? String(error),
    status: error?.status,
    error: error?.error,
    stack: error?.stack
  };
}

async function runCommand(command, commandArgs) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, commandArgs, {
      stdio: ['ignore', 'pipe', 'pipe']
    });

    let stdout = '';
    let stderr = '';

    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString();
    });

    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });

    child.on('error', reject);

    child.on('close', (code) => {
      if (code === 0) {
        resolve(stdout);
        return;
      }

      reject(new Error(`${command} exited with code ${code}\n${stderr || stdout}`.trim()));
    });
  });
}
