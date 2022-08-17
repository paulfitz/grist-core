const electron = require('electron');
const electronUpdater = require("electron-updater");

const log = require('app/server/lib/log');
const version = require('app/common/version');

class UpdateManager {
  constructor(appMenu) {
    // During auto-initiated checks (as on startup), we suppress popups for no-updates and for
    // update-check errors.
    this._suppressPopups = false;

    this._appMenu = appMenu;
    this._timeout = null;
    this._state = 'idle';

    this.autoUpdater = electronUpdater.autoUpdater;
    this.autoUpdater.logger = log;
    this.autoUpdater.autoDownload = false;

    this.autoUpdater.on('checking-for-update', () => this.onCheckingForUpdate());
    this.autoUpdater.on('update-available', (info) => this.onUpdateAvailable(info));
    this.autoUpdater.on('update-not-available', (info) => this.onUpdateNotAvailable(info));
    this.autoUpdater.on('error', (err) => this.onUpdateError(err));
    this.autoUpdater.on('download-progress', (progress) => this.onDownloadProgress(progress));
    this.autoUpdater.on('update-downloaded', (info) => this.onUpdateDownloaded(info));

    appMenu.on('check-for-updates', () => this.startManualCheck());
    appMenu.on('quit-and-install', () => this.autoUpdater.quitAndInstall());
  }

  // returns true if auto updater system is usable
  startAutoCheck() {
    if (!this.autoUpdater.loadUpdateConfig) {
      // auto updater doesn't operate on linux, and this is a symptom
      return false;
    }
    // First check if the config can be loaded, so that if not, we can report a single-line
    // warning and avoid electron-updater's big stack dump.
    if (this._state !== 'idle') { return true; }
    this._suppressPopups = true;
    this.autoUpdater.loadUpdateConfig()
    .then(
      () => this.autoUpdater.checkForUpdates(),
      (err) => log.warn("Not configured for auto-updates: %s", err.message)
    );
    return true;
  }

  schedulePeriodicChecks(seconds) {
    if (this._timeout) {
      clearInterval(this._timeout);
    }
    this._timeout = setInterval(() => this.startAutoCheck(), seconds * 1000);
  }

  startManualCheck() {
    if (this._state !== 'idle') { return; }
    this._suppressPopups = false;
    this.autoUpdater.checkForUpdates();
  }

  _setState(state) {
    this._state = state;
    this._appMenu.setUpdateState(state);
  }

  // Helper to show the progress for the first open window (shown over the dock icon).
  _showProgress(fraction) {
    let win = electron.BrowserWindow.getAllWindows()[0];
    if (win) {
      win.setProgressBar(fraction);
    }
  }

  onCheckingForUpdate() {
    log.warn('Checking for update...');
    this._setState('checking');
  }

  onUpdateAvailable(info) {
    // It would be nicer if we had a single dialog implemented as a webpage, which started with
    // this "Download Update" question, continued to "Downloading..." (with a <progress> element),
    // and ended with "Relaunch to Update" question. As it is, the only feedback the user sees
    // during download is the hard-to-find electron-supplied progress indicator over the doc icon.
    log.warn(`Update ${info.version} available; current version ${version.version}`);
    electron.dialog.showMessageBox({
      type: 'info',
      buttons: ['Download Update', 'Remind Me Later'],
      defaultId: 0,
      cancelId: 1,
      noLink: true,
      title: 'Update Available',
      message: 'A new version of Grist is available!',
      detail: `Would you like to download and install Grist ${info.version} now? ` +
        `You have ${version.version}.`
    }, (index) => {
      if (index === 0) {
        this._suppressPopups = false;
        this._showProgress(0);
        this._setState('downloading');
        this.autoUpdater.downloadUpdate();
      } else {
        this._setState('idle');
      }
    });
  }

  onUpdateNotAvailable(info) {
    log.warn('Update not available.');
    if (this._suppressPopups) {
      this._setState('idle');
    } else {
      electron.dialog.showMessageBox({
        type: 'info',
        buttons: ['OK'],
        title: 'No update available',
        message: 'No update available',
        detail: `You are at the latest version ${version.version}.`,
      }, () => {
        this._setState('idle');
      });
    }
  }

  onUpdateError(err) {
    // Clarify the error about app-update.yml.
    if (err.code === 'ENOENT' && err.message.includes('app-update.yml')) {
      err.message += ' This error is expected in development "electron-dev" builds.';
    }
    log.warn('Error in auto-updater: %s', err);
    this._showProgress(-1);
    if (this._suppressPopups) {
      this._setState('idle');
    } else {
      electron.dialog.showMessageBox({
        type: 'warning',
        buttons: ['OK'],
        title: 'Update Error',
        message: 'There was an error checking for updates.',
        detail: err.message,
      }, () => {
        this._setState('idle');
      });
    }
  }

  onDownloadProgress(p) {
    log.warn(`Downloading... ${p.percent}% (${p.transferred}/${p.total})`);
    this._showProgress(p.percent / 100);
  }

  onUpdateDownloaded(info) {
    log.warn(`Update ${info.version} downloaded`);
    this._setState('relaunch');
    // The user may have switched to another task in Grist; we need to ask again.
    electron.dialog.showMessageBox({
      type: 'info',
      buttons: ['Relaunch to Update', 'Update Later'],
      defaultId: 0,
      cancelId: 1,
      noLink: true,
      title: 'Update Ready',
      message: 'The update is ready to be installed',
      detail: `Would you like to finish updating to Grist ${info.version} now?`,
    }, (index) => {
      if (index === 0) {
        this.autoUpdater.quitAndInstall();
      }
    });
  }
}

module.exports = UpdateManager;
