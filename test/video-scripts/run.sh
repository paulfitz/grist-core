#!/usr/bin/env bash

# Run one of the video scripts, e.g.
#   test/video-scripts/run.sh _build/test/video-scripts/ScriptHomePage.js
#
# To record a video of it, run headless and pass VIDEO_RECORD=1, then turn the frames it saves
# into a video with make-video.sh:
#
#   MOCHA_WEBDRIVER_HEADLESS=1 MOCHA_WEBDRIVER_ARGS="--force-device-scale-factor=2" \
#     VIDEO_RECORD=1 VIDEO_OUT_DIR=/tmp/frames \
#     test/video-scripts/run.sh _build/test/video-scripts/ScriptHomePage.js
#   test/video-scripts/make-video.sh /tmp/frames /tmp/homepage.mp4
#
# With WAIT_ESCAPE=1 the script pauses at the start and end, waiting for the Escape key in the
# browser, which is a way to record it by other means.

set -o errexit
set -o pipefail

if [[ $# -lt 1 ]]; then
  echo "Please supply a script to run (e.g. _build/test/video-scripts/ScriptHomePage.js)"
  exit 1
fi

# Don't show any non-production plugins in videos.
export GRIST_EXPERIMENTAL_PLUGINS=0
export GRIST_USER_ROOT=/invalid

# The scripts sets its own window size; this is just a sane default for the browser.
export MOCHA_WEBDRIVER_WINSIZE=${MOCHA_WEBDRIVER_WINSIZE:-1280x900}
export MOCHA_WEBDRIVER_LOGDIR=${MOCHA_WEBDRIVER_LOGDIR:-_build/video_output}

# Keep the test server's files around, since a script is not a test.
export NO_CLEANUP=1

exec ./test/test_env.sh node_modules/.bin/mocha --slow 8000 -b --no-exit "$@"
