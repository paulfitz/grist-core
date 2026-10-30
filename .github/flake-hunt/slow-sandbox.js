/**
 * Mocha --require plugin for runs on a slow sandbox such as pyodide, adapted from
 * grist-desktop's test/electron/deployment-timeouts.js and raiseTimeouts.js. The browser
 * tests budget for a sandbox that starts in milliseconds; pyodide's first call takes
 * seconds. This puts a floor under the server waits and the declared test timeouts, so
 * what still fails is more likely a race than plain slowness.
 */

const Module = require("module");

const SERVER_FLOOR = parseInt(process.env.GRIST_TEST_SERVER_TIMEOUT || "30000", 10);
const TEST_FLOOR = parseInt(process.env.GRIST_TEST_TIMEOUT || "120000", 10);

// gristUtils binds the helpers off the prototype when it is first imported, so patch the
// prototype as soon as the module loads, before anything else sees it. Hooking the load
// rather than requiring the module here avoids pulling in mocha-webdriver early.
const origLoad = Module._load;
Module._load = function(request, ...rest) {
  const result = origLoad.call(this, request, ...rest);
  const utils = request.endsWith("/gristWebDriverUtils") && result.GristWebDriverUtils;
  if (utils && !utils.__floored) {
    utils.__floored = true;
    raiseFloor(utils, "waitForServer", 0, 5000);
    raiseFloor(utils, "sendActions", 1, 5000);
    raiseFloor(utils, "waitForDocToLoad", 0, 10000);
  }
  return result;
};

function raiseFloor(cls, method, index, upstreamDefault) {
  const orig = cls.prototype[method];
  cls.prototype[method] = function(...args) {
    args[index] = Math.max(args[index] ?? upstreamDefault, SERVER_FLOOR);
    return orig.apply(this, args);
  };
}

// Suites that declare this.timeout() beat mocha's --timeout, and mocha copies a suite's
// timeout into its hooks as they are built, so raise each runnable directly.
function raise(runnable) {
  if (runnable.timeout() > 0 && runnable.timeout() < TEST_FLOOR) {
    runnable.timeout(TEST_FLOOR);
  }
}

function raiseSuite(suite) {
  raise(suite);
  for (const kind of ["_beforeAll", "_beforeEach", "_afterEach", "_afterAll"]) {
    (suite[kind] || []).forEach(raise);
  }
  suite.tests.forEach(raise);
  suite.suites.forEach(raiseSuite);
}

exports.mochaHooks = {
  beforeAll() {
    let root = this.test.parent;
    while (root.parent) { root = root.parent; }
    raiseSuite(root);
  },
};

console.log(`[slow sandbox: server waits >= ${SERVER_FLOOR}ms, tests >= ${TEST_FLOOR}ms]`);
