# Runway Act-Two First Test

This workspace contains a small CLI for the first proof-of-concept:

- existing talking-head video as the `reference` performance
- existing male portrait image as the `character`
- Runway `act_two` only

It does four things:

1. loads the local `.env` automatically
2. prepares the target image into a tighter portrait crop
3. prepares a short source clip automatically unless you override it
4. uploads both assets to Runway, runs `character_performance`, downloads the output, and saves a review bundle locally

If you do not pass `--ratio`, the script picks the closest Runway-supported ratio to your prepared video. For a normal vertical talking-head clip, that will usually resolve to `720:1280`.

## Setup

1. Install dependencies:

```bash
npm install
```

2. Set your Runway API key:

```bash
export RUNWAYML_API_SECRET=rw_your_api_key_here
```

You can also keep it in a local `.env` file. The script will load that file automatically.

Note: Runway currently requires an account with at least one credit purchase to use ephemeral uploads through the API. If your key is valid but the request is denied before task creation, check the saved `request.failed.json` in the run folder.

## First attempt

Use the simple workflow with any local video and image:

```bash
npm run runway:simple -- \
  --video /absolute/path/to/source-video.mp4 \
  --image /absolute/path/to/character-image.heic
```

The script will create a run folder under `runs/` with:

- preflight metadata
- the prepared image
- the prepared video
- the Runway task id and output URL
- the downloaded `.mp4`
- a review checklist for pass/fail notes

## Second attempt

Override the default auto-trim with the strongest 4-6 seconds of the same take:

```bash
npm run runway:simple -- \
  --video /absolute/path/to/source-video.mp4 \
  --image /absolute/path/to/character-image.heic \
  --start 1.2 \
  --duration 5.0 \
  --label attempt-2-shorter
```

## Third attempt

Skip the default crop if you already prepared the portrait image yourself:

```bash
npm run runway:simple -- \
  --video /absolute/path/to/source-video.mp4 \
  --image /absolute/path/to/better-cropped-character.jpg \
  --no-auto-crop \
  --label attempt-3-manual-image
```

## Preflight only

Use this before spending Runway credits:

```bash
npm run runway:simple -- \
  --video /absolute/path/to/source-video.mp4 \
  --image /absolute/path/to/character-image.heic \
  --preflightOnly
```

## Basic UI

Use the local run viewer to inspect prepared assets, warnings, failed requests, and generated output:

```bash
npm run ui
```

Then open:

```text
http://localhost:3123
```

The viewer reads the saved folders under `runs/` and shows:

- generated output video when available
- prepared target image
- prepared source clip
- preflight metadata and warnings
- saved Runway API payload or failure details

## Notes on source quality

Best first-test inputs:

- `4-10s` single-speaker clip
- chest-up framing
- one face only, face always visible
- no cuts
- no hands or objects crossing the mouth
- clean lighting
- realistic portrait image with visible eyes and clear skin detail
- head + upper chest works better than full torso

Default preparation behavior:

- source clips longer than `6s` are turned into a centered `5s` prepared clip
- target images are converted to a prepared portrait crop unless you pass `--no-auto-crop`
- HEIC images are normalized before upload

If the result is weak, change one thing at a time:

1. shorten the clip
2. choose a cleaner take or manual timestamp
3. improve the image crop or replace the source image

Do not change everything at once or you will not know what helped.
