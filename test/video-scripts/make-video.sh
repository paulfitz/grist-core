#!/bin/bash

# Turn frames recorded by videoUtils.startRecording() into a video.
#
# Run as:
#   test/video-scripts/make-video.sh <frames-dir> <output.mp4> [crop]
#
# Frames arrive only when the page repaints, so frames.json records when each one was painted;
# here they are laid out on that timeline and resampled to a constant frame rate.
#
# The crop argument is an ffmpeg crop filter (e.g. "960:625:1:1") to trim the red outline that
# mktstyle=o draws around the recorded area. By default the outline is found and trimmed.

set -o errexit
set -o pipefail

FRAMES_DIR=${1:?Usage: make-video.sh <frames-dir> <output.mp4> [crop]}
OUTPUT=${2:?Usage: make-video.sh <frames-dir> <output.mp4> [crop]}
CROP=${3:-}
FPS=${FPS:-30}

LIST=$(mktemp /tmp/video-frames-XXXXXX.txt)

CROP_FILE=$(mktemp /tmp/video-crop-XXXXXX.txt)
trap 'rm -f "$LIST" "$CROP_FILE"' EXIT

python3 - "$FRAMES_DIR" "$LIST" "$CROP_FILE" <<'PYTHON'
import json, os, sys
frames_dir, list_file, crop_file = sys.argv[1], sys.argv[2], sys.argv[3]
frames = json.load(open(os.path.join(frames_dir, 'frames.json')))
with open(list_file, 'w') as out:
    for i, frame in enumerate(frames):
        path = os.path.abspath(os.path.join(frames_dir, frame['file']))
        out.write("file '%s'\n" % path)
        if i + 1 < len(frames):
            out.write("duration %.4f\n" % (frames[i + 1]['timeSec'] - frame['timeSec']))
    # The concat demuxer ignores the duration of the last frame unless it is repeated.
    out.write("file '%s'\n" % os.path.abspath(os.path.join(frames_dir, frames[-1]['file'])))
print("%d frames, %.1f sec" % (len(frames), frames[-1]['timeSec'] - frames[0]['timeSec']))

# Find the red outline that mktstyle=o draws, and report the area just inside it.
try:
    from PIL import Image
except ImportError:
    sys.exit(0)
image = Image.open(os.path.join(frames_dir, frames[0]['file'])).convert('RGB')
width, height = image.size
def is_red(pixel):
    # Lenient, since jpeg frames don't reproduce the outline color exactly.
    r, g, b = pixel
    return r > 180 and g < 80 and b < 80


def inside(values):
    """Given the red pixel positions along a line, return the span just inside the outline.
    The outline is thicker than one pixel when the browser runs at a scale factor."""
    if len(values) < 2:
        return None
    found = set(values)
    start, end = values[0], values[-1]
    while start + 1 in found:
        start += 1
    while end - 1 in found:
        end -= 1
    return (start + 1, end - start - 1) if end > start else None

xs = inside([x for x in range(width) if is_red(image.getpixel((x, height // 2)))])
ys = inside([y for y in range(height) if is_red(image.getpixel((width // 2, y)))])
if xs and ys:
    open(crop_file, 'w').write("%d:%d:%d:%d" % (xs[1], ys[1], xs[0], ys[0]))
PYTHON

if [[ -z "$CROP" && -s "$CROP_FILE" ]]; then
  CROP=$(cat "$CROP_FILE")
  echo "Cropping to the outlined area: $CROP"
fi

FILTERS="fps=${FPS}"
if [[ -n "$CROP" ]]; then
  FILTERS="crop=${CROP},${FILTERS}"
fi

ffmpeg -y -loglevel warning -f concat -safe 0 -i "$LIST" \
  -vf "${FILTERS},scale=trunc(iw/2)*2:trunc(ih/2)*2" \
  -c:v libx264 -preset slow -crf 18 -pix_fmt yuv420p -movflags +faststart "$OUTPUT"

echo "Wrote $OUTPUT"
