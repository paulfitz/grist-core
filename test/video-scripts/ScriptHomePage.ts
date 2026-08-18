/*
This is for the video shown on Grist home page.

Run this with MOCHA_WEBDRIVER_WINSIZE=1024x720 for a better screen size.
*/

import {addToRepl, driver} from 'mocha-webdriver';
import * as gu from 'test/nbrowser/gristUtils';
import {server} from 'test/nbrowser/testServer';
import {setupTestSuite} from 'test/nbrowser/testUtils';
import * as vu from 'test/video-scripts/videoUtils';

describe('ScriptHomePage', function() {
  this.timeout(600000);
  setupTestSuite();
  addToRepl('vu', vu);

  it('setup', async function() {
    await gu.setWindowDimensions(1024, 720);
    await vu.setUpOrgUser('Alice', 'alice@example.com', 'ACME', 'acme');

    // Employees doc as ACME Human Resources
    const doc = await gu.importFixturesDoc('Alice', 'acme', 'Home', 'video/Employees HomePage.grist',
      {newName: 'ACME Human Resources.grist', load: false});
    // Keep the light theme regardless of what the OS or browser prefers.
    await driver.get(server.getUrl('acme',
      `/doc/${doc.id}?mktstyle=olpf&themeSyncWithOs=false&themeAppearance=light`));
    await gu.waitForDocToLoad();

    await setLeftPanelWidth(200);

    vu.waitInit(7);
    await vu.mouseInit();
    await vu.initCallouts();
    await vu.mouseMoveClick(0.5, gu.getCell({section: 'All Employees', rowNum: 3, col: 'Employee Name'}));
    await vu.waitForEscape();
    await vu.startRecording();
  });

  it('linking', async function() {
    // In Employee View, click rows 2, 3
    await vu.mouseMoveClick(0.5, gu.getCell({section: 'All Employees', rowNum: 1, col: 'Employee Name'}));
    await vu.showCallout( { text: 'Linked views', x: 274, y: 288 } );
    await driver.sleep(500);
    await vu.mouseMoveClick(0.5, gu.getCell({section: 'All Employees', rowNum: 3, col: 'Employee Name'}));
    await driver.sleep(500);
    await vu.mouseMoveClick(0.5, gu.getCell({section: 'All Employees', rowNum: 2, col: 'Employee Name'}));
    await vu.showCallout( { text: 'See records as cards', x: 579, y: 338 } );
    await driver.sleep(500);
    await vu.mouseMoveClick(0.5, gu.getCell({section: 'All Employees', rowNum: 3, col: 'Employee Name'}));
    await vu.mouseMoveClick(0.5, gu.getDetailCell({section: 'Employee Card', rowNum: 1, col: 'Employee Name'}));
    await vu.mouseMoveClick(0.5, gu.getDetailCell({section: 'Employee Card', rowNum: 1, col: 'Photo'}));
    await vu.mouseMoveClick(0.5, gu.getDetailCell({section: 'Employee Card', rowNum: 1, col: 'PerformanceScore'}));
    await driver.sleep(500);
  });

  it('dashboards', async function() {
    // Click Department View
    await vu.hideCallout();
    await vu.mouseMoveClick(0.75, gu.getPageItem('Department View'));
    await vu.showCallout( { text: 'Dynamic dashboards', x: 192, y: 317 } );

    // Click rows 2, 3, 4
    // await vu.mouseMove(0.5, gu.getCell({section: 'Departments', rowNum: 1, col: 'Department'}));
    // await driver.sleep(250);
    await vu.mouseMoveClick(0.5, gu.getCell({section: 'Departments', rowNum: 2, col: 'Department'}));
    await driver.sleep(250);
    await vu.showCallout( { text: '...and charts', x: 362, y: 385, delaySec: 0.5} );
    await vu.mouseMoveClick(0.5, gu.getCell({section: 'Departments', rowNum: 3, col: 'Department'}));
    await driver.sleep(250);
    await vu.mouseMoveClick(0.5, gu.getCell({section: 'Departments', rowNum: 4, col: 'Department'}));
    await driver.sleep(500);
    await vu.hideCallout();
  });

  it('create views', async function() {
    // Click Add New -> Widget to page
    await vu.waitSec(0.5);
    await vu.showCallout( { text: 'Create useful views', x: 208, y: 44, delaySec: 0.5 } );
    await vu.mouseMoveClick(0.5, driver.find('.test-dp-add-new'), {pause: 0.5});
    await vu.waitSec(0.5);
    await vu.mouseMoveClick(0.5, driver.findWait('.test-dp-add-widget-to-page', 500), {pause: 0.5});
    await vu.hideCallout();

    await vu.waitSec(0.5);
    await vu.showCallout( { text: 'Summarize data', x: 431, y: 190, delaySec: 0.5 } );
    await vu.mouseMoveClick(0.5, driver.findContent('.test-wselect-table', 'Employees'));
    await vu.mouseMoveClick(0.25, driver.findContent('.test-wselect-table', 'Employees')
      .find('.test-wselect-pivot'), {pause: .5});
    await vu.waitSec(0.25);
    await vu.mouseMoveClick(0.25, driver.findContent('.test-wselect-column', 'State'), {pause: 0.4, x: -60});

    // Just show the dialog, skip the actual extra widget.
    await vu.waitSec(0.5);
    // await vu.mouseMove(0.4, driver.findWait('.test-wselect-addBtn', 500));
    // await vu.waitSec(0.25);
    await vu.hideCallout();
    await vu.mouseMoveClick(0.4, driver.findWait('.test-wselect-addBtn', 500), {x: -50, y: 130});
  });

  it('arrange', async function() {
    // Drag chart section above the employees section.
    await vu.showCallout( { text: 'Arrange views easily', x: 357, y: 258, delaySec: 0.5 } );
    await vu.mouseMove(0.45, gu.getSection('Spending by Role').find('.viewsection_drag_indicator'), {x: 8});
    await vu.mouseMove(0.05, gu.getSection('Spending by Role').find('.viewsection_drag_indicator'), {x: 0});
    await vu.waitSec(0.25);
    await driver.mouseDown();
    await vu.mouseMove(0.5, gu.getSection('Department Staff'), {x: 0, y: -65});
    await vu.waitSec(0.25);
    await driver.mouseUp();

    await gu.waitForServer();
    await vu.waitSec(1);

    await vu.mouseMoveClick(0.5, gu.getCell({section: 'Departments', rowNum: 3, col: 'Department'}));
    await vu.waitSec(0.25);
    await vu.mouseMoveClick(0.5, gu.getCell({section: 'Departments', rowNum: 2, col: 'Department'}));
    await vu.waitSec(0.25);

    // await vu.mouseMove(0.5, gu.getSection('Department Staff').find('.test-viewsection-title'));
    await vu.mouseMove(0.45, gu.getSection('Department Staff').find('.viewsection_drag_indicator'), {x: 8});
    await vu.mouseMove(0.05, gu.getSection('Department Staff').find('.viewsection_drag_indicator'), {x: 0});
    await vu.waitSec(0.25);
    await driver.mouseDown();
    await vu.hideCallout();
    await vu.mouseMove(0.5, gu.getSection('Departments'), {x: 40, y: 210});
    await vu.waitSec(0.25);
    await driver.mouseUp();

    await gu.waitForServer();
    await vu.waitSec(1);
  });

  it('more linking', async function() {
    // Click some rows in Departments
    await vu.mouseMoveClick(0.5, gu.getCell({section: 'Departments', rowNum: 1, col: 'Department'}));
    await vu.showCallout( { text: 'Organize your data, your way', x: 266, y: 272 } );
    await vu.waitSec(0.5);
    await vu.mouseMoveClick(0.5, gu.getCell({section: 'Departments', rowNum: 2, col: 'Department'}));
    await vu.waitSec(0.5);

    // Click Employee View page
    await vu.mouseMoveClick(0.5, gu.getPageItem('Employee View'));

    // Click row 1 there.
    await vu.mouseMoveClick(0.5, gu.getCell({section: 'All Employees', rowNum: 1, col: 'Employee Name'}));
    await driver.sleep(500);
    await vu.hideCallout();
    await vu.mouseMoveClick(0.5, gu.getCell({section: 'All Employees', rowNum: 3, col: 'Employee Name'}));
    await driver.sleep(500);
  });

  it('end', async function() {
    await vu.stopRecording();
    await vu.waitForEscape();
  });
});

// Change the width of the left panel, which is 240px wide by default.
async function setLeftPanelWidth(width: number) {
  if (await driver.executeScript(() => window.sessionStorage.getItem('leftPanelWidth')) !== String(width)) {
    await driver.executeScript((w: string) => window.sessionStorage.setItem('leftPanelWidth', w), String(width));
    await driver.sleep(500);
    await driver.navigate().refresh();
    await gu.waitForDocToLoad();
  }
}
