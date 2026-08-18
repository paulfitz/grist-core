/**
 * Utilities for scripts that go through some functionality for recording a video of it.
 *
 * If waitForEscape() is used in the script, then run it with WAIT_ESCAPE=1 env var. In this case,
 * the test pauses when it's ready to start recording and waits for you to hit Escape in the
 * browser. At the end, it waits for Escape again to exit.
 *
 * To display mouse motions and clicks, use mouseInit() on any new page to display the mouse, then
 * mouseMove() and mouseClick().
 *
 * To sync with voiceover, use waitInit() to start the timing, then waitTill(sec) to mark the time
 * in the audio (in seconds from the start) until which to wait before the next script action.
 */
import {assert, driver, WebElement, WebElementPromise} from 'mocha-webdriver';
import * as gu from 'test/nbrowser/gristUtils';
import {server} from 'test/nbrowser/testUtils';

/**
 * Set up by an org and log in as user in that org.
 * E.g. setUpOrgUser('Alice', 'alice@example.com', 'ACME', 'acme');
 */
export async function setUpOrgUser(userName: string, email: string, orgName: string, domain: string) {
  await server.simulateLogin(userName, email, domain, {isFirstLogin: false});
  await driver.get(`${server.getHost()}/o/${domain}`);
  await driver.findWait('.test-dm-org', 3000);
  if (await driver.find('.test-error-header').isPresent()) {
    const homeApi = gu.createHomeApi(userName, 'docs');
    await gu.setApiKey(userName);
    await homeApi.newOrg({name: orgName, domain});
    await server.simulateLogin(userName, email, domain);

    // Upgrade this org to a fancier plan.
    await gu.updateOrgPlan(orgName, 'team');

    await driver.get(`${server.getHost()}/o/${domain}`);
    await driver.findWait('.test-dm-org', 3000);
    assert.equal(await driver.find('.test-error-header').isPresent(), false);
  }
}

const loadPrompt = function() {
  const elem = document.createElement('div');
  Object.assign(elem.style, {
    position: 'absolute',
    textAlign: 'center',
    bottom: '20px',
    width: '100%',
    fontFamily: 'sans-serif',
    fontSize: '22px',
    fontWeight: 'bold',
    textShadow: '0px 0px 5px yellow',
    color: 'blue',
    whiteSpace: 'pre-line',
  });
  document.body.appendChild(elem);
  (window as any).videoPrompt = (text: string) => { elem.textContent = text; };
};

const loadKeyWait = function() {
  const elem = document.createElement('div');
  elem.textContent = 'Hit Space';
  Object.assign(elem.style, {
    position: 'absolute',
    display: 'none',
    textAlign: 'center',
    bottom: '10px',
    right: '10px',
    border: '2px solid blue',
    backgroundColor: 'yellow',
    fontFamily: 'sans-serif',
    fontSize: '22px',
    color: 'blue',
    fontWeight: 'bold',
    padding: '15px',
  });
  document.body.appendChild(elem);

  (window as any).waitForSpace = (cb: any) => {
    function f(ev: any) {
      if (ev.key === ' ') {
        ev.preventDefault();
        window.removeEventListener('keydown', f);
        elem.style.display = 'none';
        cb();
      }
    }
    elem.style.display = 'block';
    window.addEventListener('keydown', f);
  };
};

const loadMouseTracking = `
  const cursor = document.createElement('div');
  Object.assign(cursor.style, {
    position: 'absolute',
    pointerEvents: 'none',
    backgroundColor: '#9a81ff60',
    width: '24px',
    height: '24px',
    borderRadius: '24px',
    border: '2px solid #9a81ff',
    boxShadow: '0 0 0 2px white',
    zIndex: 1000000, /* beat ace's of 200000 */
    display: 'none',
  });
  document.body.appendChild(cursor);
  function delay(ms) { return new Promise(r => setTimeout(r, ms)); }
  const w = window;
  w.mouseCursor = cursor;
  w.mousePos = {x: -100, y: -100};
  w.mouseShow = function(yesNo) { cursor.style.display = yesNo ? 'block' : 'none'; };
  w.mouseMove = async function(durationMs, pos) {
    const start = Date.now();
    const deltaX = mousePos.x - pos.x;
    const deltaY = mousePos.y - pos.y;
    Object.assign(cursor.style, {
      left: (pos.x - 12) + 'px',
      top: (pos.y - 12) + 'px',
      transform: 'translate(' + deltaX + 'px, ' + deltaY + 'px)',
      transition: (durationMs / 1000) + 's',
    });
    cursor.style.transform = 'none';
    await delay(durationMs);
    cursor.style.transition = 'none';
    w.mousePos = pos;
  };
  w.mouseClick = async function() {
    const {x, y} = window.mousePos;
    Object.assign(cursor.style, {
      transition: '0.1s',
      width: '8px',
      height: '8px',
      left: (x - 4) + 'px',
      top: (y - 4) + 'px',
    });
    await delay(100);
    Object.assign(cursor.style, {
      width: '24px',
      height: '24px',
      left: (x - 12) + 'px',
      top: (y - 12) + 'px',
    });
    await delay(100);
    cursor.style.transition = 'none';
  };
`;

// Describes the interface which we attach inside the browser to 'window' using loadMouseTracking,
// for displaying mouse actions.
interface IMouseTrack extends Window {
  mouseCursor: WebElement;
  mousePos: {x: number, y: number};
  mouseShow(yesNo: boolean): void;
  mouseMove(durationMs: number, pos: {x: number, y: number}): Promise<void>;
  mouseClick(): Promise<void>;
}

interface IMousePos {
  x: number;
  y: number;
}

interface IOffset {
  x?: number;
  y?: number;
  scroll?: ScrollIntoViewOptions;   // If set (e.g. to {}), will default {block: 'center', behavior: 'smooth'}
}

// Dummy variable for type-checking usage of window methods that are actually used via
// executeScript inside the browser.
const window: IMouseTrack = null as any;

/**
 * Initialize mouse tracking, and position the mouse at the given element, defaulting to the
 * center of document.body.
 *
 * Also initializes other helpers, for the prompt, and for waiting.
 */
export async function mouseInit(elem?: WebElement|IMousePos) {
  await driver.executeScript(loadMouseTracking);
  if (process.env.SHOW_PROMPTS !== '0') {
    await driver.executeScript(loadPrompt);
  }
  await driver.executeScript(loadKeyWait);
  if (!elem || elem instanceof WebElement) {
    await mouseMove(0, elem || driver.find('body'));
  } else {
    const rect = await driver.find('body').rect();
    const bodyOffset = {
      x: elem.x - rect.left - rect.width / 2,
      y: elem.y - rect.top - rect.height / 2,
    };
    await mouseMove(0, driver.find('body'), bodyOffset);
  }
  await driver.executeScript(() => window.mouseShow(true));
}

export async function mousePos(): Promise<IMousePos> {
  return driver.executeScript(() => window.mousePos);
}

export async function mouseShow(yesNo: boolean) {
  await driver.executeScript((_yesNo: boolean) => window.mouseShow(_yesNo), yesNo);
}

/**
 * Move the mouse slowly, with tracking, to the given element, with an optional offset from its
 * center. Returns the ending position of the mouse.
 */
export async function mouseMove(
  durationSec: number, elem: WebElement|'current', offset: IOffset = {}
): Promise<IMousePos> {
  const durationMs = durationSec * 1000;
  const curPos: IMousePos = await mousePos();
  let endPos: IMousePos;
  if (elem === 'current') {
    endPos = {
      x: curPos.x + (offset.x || 0),
      y: curPos.y + (offset.y || 0),
    };
  } else {
    if (offset.scroll) {
      await scrollIntoView(elem, offset.scroll);
    }
    const rect = await elem.rect();
    const x = rect.left + (rect.width / 2) + (offset.x || 0);
    const y = rect.top + (rect.height / 2) + (offset.y || 0);
    endPos = {x, y};
  }

  // This triggers a transition lasting durationMs but doesn't actually wait for it.
  await driver.executeScript((ms: number, pos: any) => { void(window.mouseMove(ms, pos)); }, durationMs, endPos);

  // sync() moves the actual mouse to the current position of the visual cursor (which is moving
  // using css transitions). This convoluted setup mainly arose during evolution of this code, but
  // might nevertheless be a good way to show a smooth motion.

  // Webdriver doesn't currently support moving to absolute position, so we move relative to
  // <body> instead.
  const body = await driver.find('body');
  const bodyRect = await body.rect();
  const bodyX = bodyRect.left + bodyRect.width / 2;
  const bodyY = bodyRect.top + bodyRect.height / 2;

  let done = false;
  async function sync() {
    while (!done) {
      await driver.sleep(20);
      // Get the position of the visual cursor during its transition to endPos.
      const el = new WebElementPromise(driver, driver.executeScript(() => window.mouseCursor));
      const r = await el.rect();
      const x = r.left + 12;
      const y = r.top + 12;
      // TODO: duration: 0 is a hack; duration should be supported by mocha-webdriver (only a
      // typings issue) and should default to 0 to match previous behavior. Some upgrades changed
      // behavior (duration default is 100 but it was ignored until something got upgraded).
      await body.mouseMove({x: Math.round(x - bodyX), y: Math.round(y - bodyY), duration: 0} as any);
      Object.assign(curPos, {x, y});
    }
    await body.mouseMove({x: Math.round(endPos.x - bodyX), y: Math.round(endPos.y - bodyY), duration: 0} as any);
  }

  // Once the duration completes, end the sync() loop.
  await Promise.all([sync(), driver.sleep(durationMs).then(() => { done = true; })]);
  return endPos;
}

/**
 * Scroll element into view. Defaults to {block: 'center', behavior: 'smooth'}
 */
export async function scrollIntoView(elem: WebElement, scroll: ScrollIntoViewOptions) {
  let rect = await elem.rect();
  const scrollOpts = {block: 'center', behavior: 'smooth', ...scroll};
  await driver.executeScript((el: any, opt: any) => el.scrollIntoView(opt), elem, scrollOpts);
  // Wait for the scrolling to end by checking that element position stops changing.
  while (true) {  // eslint-disable-line no-constant-condition
    await driver.sleep(50);
    const newRect = await elem.rect();
    if (newRect.top === rect.top && newRect.left === rect.left) {
      break;
    }
    rect = newRect;
  }
}

/**
 * Click the element given after a delay.
 */
export async function mouseClick(delaySec: number, elem: WebElement) {
  const delayMs = delaySec * 1000;
  await driver.sleep(delayMs);
  await driver.executeAsyncScript((cb: any) => window.mouseClick().then(cb));
  await elem.click();
}

export async function mouseDoubleClick(elem: WebElement) {
  // Make the double-click look like a double-click.
  await driver.executeAsyncScript((cb: any) => window.mouseClick().then(cb));
  await driver.executeAsyncScript((cb: any) => window.mouseClick().then(cb));
  await driver.withActions(a => a.doubleClick(elem));
}

/**
 * Shortcut for mouseMove() followed by mouseClick(), with 0.1 default delay on the latter.
 * Note that the click itself takes ~0.2 sec, for a total of 0.3 sec by default.
 * Returns the ending position of the mouse.
 */
export async function mouseMoveClick(
  delaySec: number, elem: WebElement|'current',
  {x, y, pause = 0.1}: {x?: number, y?: number, pause?: number} = {}
): Promise<IMousePos> {
  const endPos = await mouseMove(delaySec, elem, {x, y});

  // Show a click but use mouseDown+mouseUp, so that the click happens wherever the mouse is,
  // which may be outside the element, or relative to current position.
  await driver.sleep(pause * 1000);
  await Promise.all([
    // In parallel send the actual click (press + release) and show it visually.
    driver.sleep(100).then(() => driver.withActions((a) => a.press().release())),
    driver.executeAsyncScript((cb: any) => window.mouseClick().then(cb)),
  ]);
  return endPos;
}

// Wait for Escape key to be pressed in the browser before continuing.
// Only if WAIT_ESCAPE env var is set. When a test suite is intended for recording a video, this
// can be used to pause at the beginning and end of the script, to allow starting and stopping the
// video during these pauses.
export async function waitForEscape() {
  if (process.env.WAIT_ESCAPE) {
    await driver.manage().setTimeouts({script: 120000});
    console.log("Waiting for Escape key in the browser");
    await driver.executeAsyncScript((cb: any) => {
      function f(ev: any) { if (ev.key === 'Escape') { window.removeEventListener('keydown', f, true); cb(); } }
      window.addEventListener('keydown', f, true);
    });
  } else {
    console.log("Run with WAIT_ESCAPE=1 to pause here for in-browser Escape key");
  }
}

let startSec = 0;

/**
 * Initialize start time of video, for waitTill() methods. Specify an intro length if sec in
 * waitTill(sec) will include an offset (e.g. there is a 5-second intro before this script starts,
 * then timestamp of 7 should actually happen 2 seconds after start, so use waitInit(5) and
 * waitTill(7) to wait 2 seconds).
 *
 * The strategy is to listen to audio, marking offsets when certain screen actions should happen,
 * and use those offsets in waitTill(). Then the video produced by the script will not require
 * further manual editing to match to the audio.
 */
export function waitInit(introLenSec: number = 0) {
  startSec = Date.now() / 1000 - introLenSec;
}

/**
 * Wait until the given point in video, in seconds. The value is a time since waitInit() was
 * called (plus any intro length given as waitInit argumnet).
 */
export async function waitTill(sec: number) {
  const now = Date.now() / 1000;
  console.log(`Now at ${now - startSec} sec, waiting till ${sec}`);
  await driver.sleep((startSec + sec - now) * 1000);
}

/**
 * Like driver.sleep() but using seconds (instead of milliseconds) for consistency with waits here.
 * When sec is null, wait for user to hit space, and print out what value to replace the null
 * argument with to match this delay.
 */
export async function waitSec(sec: number|null) {
  if (sec !== null) {
    await driver.sleep(sec * 1000);
  } else {
    const err = new Error();
    Error.captureStackTrace(err, waitSec);
    const line = err.stack!.split(/\n/).find(ln => ln.includes('video-scripts'));
    const lineNum = line!.split(':')[1];
    const start = Date.now();
    await driver.executeAsyncScript((cb: any) => (window as any).waitForSpace(cb));
    const end = Date.now();
    const elapsed = Math.round((end - start) / 100) / 10;
    console.log(`>>> Line ${lineNum}: await vu.waitSec(${elapsed});`);
  }
}

/**
 * Enter text slowly, with a wait between keys.
 */
export async function slowKeys(text: string, msecBetweenKeys: number = 50) {
  for (const letter of text) {
    await driver.sendKeys(letter);
    await driver.sleep(msecBetweenKeys);
  }
}


/**
 * Show the given prompt at the bottom of the page.
 * mouseInit() must be called on a page before this can be used.
 * Use SHOW_PROMPTS=0 to disable.
 */
export async function prompt(text: string) {
  if (process.env.SHOW_PROMPTS !== '0') {
    text = text.split(/\n/).map(l => l.trim()).join("\n");
    await driver.executeScript((t: string) => (window as any).videoPrompt(t), text);
  }
}

export async function setHeight(hFraction?: number) {
  await gu.setWindowDimensions(1024, (hFraction || 1) * 640);
}

const loadCallouts = function() {
  const style = document.createElement('style');
  style.textContent = `
    .script-callout {
        position: absolute;
        text-align: center;
        font-family: var(--grist-font-family-data);
        font-size: 32px;
        white-space: pre-line;
        left: 400px;
        border-radius: 50px;
        padding: 8px 24px;
        background-color: black;
        color: white;
        box-shadow: 1px 1px 5px 1px grey, 0 0 10px 10px white;
        z-index: 1000;
        opacity: 0;
        transition: opacity;
        pointer-events: none;
    }
    `;
  document.head.appendChild(style);

  const elem = document.createElement('div');
  elem.classList.add('script-callout');
  document.body.appendChild(elem);

  (window as any).showCallout =
    ({text, x = 0, y = 0, fadeIn = 0.4}: {text: string, x: number, y: number, fadeIn: number}) => {
      function show() {
        elem.textContent = text;
        elem.style.left = `${x}px`;
        elem.style.top = `${y}px`;
        elem.style.transitionDuration = `${fadeIn}s`;
        elem.style.opacity = '1';
      }
      if (elem.style.opacity !== '0') {
        (window as any).hideCallout();
        setTimeout(show, 400);
      } else {
        show();
      }
    };
  (window as any).hideCallout =
    ({fadeOut = 0.4}: {fadeOut?: number} = {}) => {
      elem.style.transitionDuration = `${fadeOut}s`;
      elem.style.opacity = '0';
    };
  (window as any).positionCallout = (cb: any) => {
    elem.style.pointerEvents = 'unset';
    elem.addEventListener('mousedown', down);
    function down(ev0: MouseEvent) {
      const rect = elem.getBoundingClientRect();
      const deltaX = ev0.screenX - rect.left;
      const deltaY = ev0.screenY - rect.top;
      function move(ev: MouseEvent) {
        elem.style.left = `${ev.screenX - deltaX}px`;
        elem.style.top = `${ev.screenY - deltaY}px`;
      }
      function up(ev: MouseEvent) {
        cb({x: ev.screenX - deltaX, y: ev.screenY - deltaY});
        elem.removeEventListener('mousedown', down);
        window.removeEventListener('mousemove', move);
        window.removeEventListener('mouseup', up);
        elem.style.pointerEvents = '';
      }
      window.addEventListener('mousemove', move);
      window.addEventListener('mouseup', up);
    }
  };
};

export async function initCallouts() {
  await driver.executeScript(loadCallouts);
}

// Show callout, without waiting for it to fade in. If another one is already shown, the new one
// will wait for it to fade out, but the call will return without waiting.
//
// If 'manual: true' is given, to allow dragging the callout manually, waiting until mouseup; then
// print its {x, y} position to the test's stdout, for use with showCallout().
//
// If 'delaySec' is given, will wait this many seconds before starting to fade in the callout, but
// will return immediately.
export async function showCallout(
  options: {text: string, x?: number, y?: number, fadeIn?: number, delaySec?: number, manual?: boolean}) {

  const showCalloutOnly = () => driver.executeScript((opt: any) => (window as any).showCallout(opt), options);
  if (options.delaySec && !options.manual) {
    delayedAsyncCall(options.delaySec, showCalloutOnly);
  } else {
    await showCalloutOnly();
  }
  if (options.manual) {
    const position: any = await driver.executeAsyncScript((cb: any) => (window as any).positionCallout(cb));
    const newOptions = {...options, ...position};
    delete newOptions.manual;
    console.log('await vu.showCallout(', newOptions, ');');
  }
}
export async function hideCallout(options: {fadeOut?: number} = {}) {
  await driver.executeScript((opt: any) => (window as any).hideCallout(opt), options);
}

// Delays func() call without waiting for either the delay or for the call to resolve.
export function delayedAsyncCall(delaySec: number, func: () => unknown): void {
  void(driver.sleep(delaySec * 1000).then(func));
}

/**
 * Screen recording, for running headless. Chrome's DevTools protocol streams frames of the page
 * as they are painted, which works without any display attached. Frames arrive irregularly (only
 * on repaint), so their timestamps are saved alongside them, and make-video.sh turns the result
 * into a constant-rate video.
 *
 * We talk to the DevTools endpoint directly rather than through selenium's createCDPConnection(),
 * which is unavailable once BiDi is enabled (as it is for Grist's browser tests).
 */
interface IRecording {
  ws: any;
  dir: string;
  frames: Array<{file: string, timeSec: number}>;
  send(method: string, params?: object): void;
}

let recording: IRecording|undefined;

/**
 * Start recording the page. Frames and a frames.json index are written to dir (defaulting to
 * $VIDEO_OUT_DIR, or _testoutputs/video). Does nothing unless VIDEO_RECORD is set.
 */
export async function startRecording(dir?: string) {
  if (!process.env.VIDEO_RECORD) {
    console.log("Run with VIDEO_RECORD=1 to record the screen here");
    return;
  }
  const fs = await import('fs');
  const path = await import('path');
  const {default: WebSocket} = await import('ws');

  const outDir = dir || process.env.VIDEO_OUT_DIR || '_build/video_output/frames';
  // PNG frames are lossless but bulky; VIDEO_FORMAT=jpeg trades a little quality for much less
  // disk (worth it for long recordings).
  const format = process.env.VIDEO_FORMAT === 'jpeg' ? 'jpeg' : 'png';
  const quality = Number(process.env.VIDEO_QUALITY || 92);
  const extension = format === 'jpeg' ? 'jpg' : 'png';
  fs.rmSync(outDir, {recursive: true, force: true});
  fs.mkdirSync(outDir, {recursive: true});

  // Chromedriver reports the browser's DevTools address as a capability.
  const caps = await driver.getCapabilities();
  const address = (caps.get('goog:chromeOptions') as any)?.debuggerAddress;
  if (!address) { throw new Error("No DevTools address; is the browser Chrome?"); }
  const targets = await fetch(`http://${address}/json/list`).then(r => r.json()) as any[];
  const target = targets.find(t => t.type === 'page');
  if (!target) { throw new Error("No page target to record"); }

  const ws = new WebSocket(target.webSocketDebuggerUrl);
  let nextId = 1;
  const rec: IRecording = {
    ws, dir: outDir, frames: [],
    send(method: string, params: object = {}) { ws.send(JSON.stringify({id: nextId++, method, params})); },
  };
  ws.on('message', (data: Buffer) => {
    let payload: any;
    try {
      payload = JSON.parse(data.toString());
    } catch (err) {
      return;
    }
    if (payload.method !== 'Page.screencastFrame') { return; }
    const {data: base64, metadata, sessionId} = payload.params;
    const file = `frame-${String(rec.frames.length).padStart(5, '0')}.${extension}`;
    fs.writeFileSync(path.join(outDir, file), Buffer.from(base64, 'base64'));
    rec.frames.push({file, timeSec: metadata.timestamp});
    rec.send('Page.screencastFrameAck', {sessionId});
  });
  await new Promise((resolve, reject) => { ws.on('open', resolve); ws.on('error', reject); });
  rec.send('Page.startScreencast', {format, quality, everyNthFrame: 1});
  recording = rec;
  console.log(`Recording frames to ${outDir}`);
}

/**
 * Stop recording, and write out the index of frames with their timestamps.
 */
export async function stopRecording() {
  if (!recording) { return; }
  const rec = recording;
  recording = undefined;
  rec.send('Page.stopScreencast');
  // Frames in flight may still arrive; give them a moment before writing the index.
  await driver.sleep(500);
  const fs = await import('fs');
  const path = await import('path');
  fs.writeFileSync(path.join(rec.dir, 'frames.json'), JSON.stringify(rec.frames, null, 2));
  rec.ws.close();
  console.log(`Recorded ${rec.frames.length} frames to ${rec.dir}`);
}
