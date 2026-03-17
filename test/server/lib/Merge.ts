import { UserAPI } from "app/common/UserAPI";
import { TestServer } from "test/gen-server/apiUtils";
import { createTmpDir } from "test/server/docTools";
import * as testUtils from "test/server/testUtils";

import { assert } from "chai";
import fetch from "node-fetch";

describe("Merge", function() {
  this.timeout(40000);
  let server: TestServer;
  let owner: UserAPI;
  let wsId: number;
  let oldEnv: testUtils.EnvironmentSnapshot;
  let oldLogLevel: testUtils.NestedLogLevel;

  before(async function() {
    oldLogLevel = testUtils.nestLogLevel("error");
    oldEnv = new testUtils.EnvironmentSnapshot();
    const tmpDir = await createTmpDir();
    process.env.GRIST_DATA_DIR = tmpDir;
    server = new TestServer(this);
    await server.start(["home", "docs"]);
    const api = await server.createHomeApi("chimpy", "docs", true);
    await api.newOrg({ name: "mergetesty", domain: "mergetesty" });
    owner = await server.createHomeApi("chimpy", "mergetesty", true);
    wsId = await owner.newWorkspace({ name: "ws" }, "current");
  });

  after(async function() {
    const api = await server.createHomeApi("chimpy", "docs");
    await api.deleteOrg("mergetesty");
    await server.stop();
    oldEnv.restore();
    oldLogLevel.restore();
  });

  /**
   * Helper to call POST /api/docs/:docId/merge directly.
   */
  async function callMerge(docId: string, body: any): Promise<any> {
    const resp = await callMergeRaw(docId, body);
    const result = await resp.json();
    if (resp.status !== 200) {
      throw new Error(`Merge failed (${resp.status}): ${JSON.stringify(result)}`);
    }
    return result;
  }

  /** Like callMerge but returns the raw Response (for testing error codes). */
  async function callMergeRaw(docId: string, body: any) {
    return fetch(`${server.serverUrl}/o/mergetesty/api/docs/${docId}/merge`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": "Bearer api_key_for_chimpy",
      },
      body: JSON.stringify(body),
    });
  }

  it("merges non-overlapping row additions", async function() {
    const docId = await owner.newDoc({ name: "merge-basic" }, wsId);
    const docApi = owner.getDocAPI(docId);

    // Seed data.
    await docApi.addRows("Table1", {
      A: ["original"],
      B: [100],
    });

    // Fork and edit both sides.
    const forkResult = await docApi.fork();
    const forkId = forkResult.urlId;
    const forkApi = owner.getDocAPI(forkId);

    // Target adds a row.
    await docApi.addRows("Table1", { A: ["trunk-row"], B: [200] });

    // Source adds a row.
    await forkApi.addRows("Table1", { A: ["fork-row"], B: [300] });

    // Merge fork into trunk.
    const result = await callMerge(docId, { sourceDocId: forkId });

    assert.isTrue(result.applied);
    assert.equal(result.conflicts.cells.length, 0);
    assert.equal(result.conflicts.rows.length, 0);

    // Verify both rows are present.
    const rows = await docApi.getRows("Table1");
    const names = rows.A;
    assert.include(names, "original");
    assert.include(names, "trunk-row");
    assert.include(names, "fork-row");
    assert.lengthOf(names, 3);
  });

  it("detects cell conflicts and applies with resolution", async function() {
    const docId = await owner.newDoc({ name: "merge-conflict" }, wsId);
    const docApi = owner.getDocAPI(docId);

    await docApi.addRows("Table1", { A: ["start"], B: [100] });

    const forkResult = await docApi.fork();
    const forkId = forkResult.urlId;
    const forkApi = owner.getDocAPI(forkId);

    // Both sides update the same cell.
    await docApi.updateRows("Table1", { id: [1], A: ["trunk-edit"] });
    await forkApi.updateRows("Table1", { id: [1], A: ["fork-edit"] });

    // First call: discover conflicts.
    const result1 = await callMerge(docId, { sourceDocId: forkId });
    assert.isFalse(result1.applied);
    assert.lengthOf(result1.conflicts.cells, 1);
    assert.equal(result1.conflicts.cells[0].left, "trunk-edit");
    assert.equal(result1.conflicts.cells[0].right, "fork-edit");

    // Second call: resolve with "right" (take fork's value).
    const result2 = await callMerge(docId, {
      sourceDocId: forkId,
      resolutions: {
        cells: [{
          tableId: "Table1", colId: "A", rowId: 1, pick: "right",
        }],
        rows: [],
      },
    });
    assert.isTrue(result2.applied);

    // Verify fork's value was applied.
    const rows = await docApi.getRows("Table1");
    assert.equal(rows.A[0], "fork-edit");
  });

  it("handles row ID collisions when both sides add rows", async function() {
    const docId = await owner.newDoc({ name: "merge-remap" }, wsId);
    const docApi = owner.getDocAPI(docId);

    await docApi.addRows("Table1", { A: ["seed"], B: [1] });

    const forkResult = await docApi.fork();
    const forkId = forkResult.urlId;
    const forkApi = owner.getDocAPI(forkId);

    // Both sides add 3 rows — they'll allocate the same rowIds (2, 3, 4).
    await docApi.addRows("Table1", { A: ["t1", "t2", "t3"], B: [10, 20, 30] });
    await forkApi.addRows("Table1", { A: ["f1", "f2", "f3"], B: [40, 50, 60] });

    const result = await callMerge(docId, { sourceDocId: forkId });
    assert.isTrue(result.applied);

    // Should have 1 seed + 3 trunk + 3 fork = 7 rows.
    const rows = await docApi.getRows("Table1");
    assert.lengthOf(rows.A, 7);
    assert.include(rows.A, "seed");
    assert.include(rows.A, "t1");
    assert.include(rows.A, "t2");
    assert.include(rows.A, "t3");
    assert.include(rows.A, "f1");
    assert.include(rows.A, "f2");
    assert.include(rows.A, "f3");

    // Verify no duplicates.
    assert.equal(new Set(rows.id).size, 7);
  });

  it("returns applied:false with empty conflicts when nothing to merge", async function() {
    const docId = await owner.newDoc({ name: "merge-noop" }, wsId);
    const docApi = owner.getDocAPI(docId);

    await docApi.addRows("Table1", { A: ["data"] });

    const forkResult = await docApi.fork();
    const forkId = forkResult.urlId;

    // Neither side makes changes after forking.
    const result = await callMerge(docId, { sourceDocId: forkId });
    assert.isFalse(result.applied);
    assert.equal(result.conflicts.cells.length, 0);
  });

  it("convergent edits are not flagged as conflicts", async function() {
    const docId = await owner.newDoc({ name: "merge-converge" }, wsId);
    const docApi = owner.getDocAPI(docId);

    await docApi.addRows("Table1", { A: ["old"], B: [100] });

    const forkResult = await docApi.fork();
    const forkId = forkResult.urlId;
    const forkApi = owner.getDocAPI(forkId);

    // Both sides make the same edit.
    await docApi.updateRows("Table1", { id: [1], A: ["same-value"] });
    await forkApi.updateRows("Table1", { id: [1], A: ["same-value"] });

    const result = await callMerge(docId, { sourceDocId: forkId });
    // No conflicts — convergent edit.
    assert.isFalse(result.applied);  // Nothing to apply, target already has the value.
    assert.equal(result.conflicts.cells.length, 0);
  });

  it("supports single-call merge with blanket strategy", async function() {
    const docId = await owner.newDoc({ name: "merge-blanket" }, wsId);
    const docApi = owner.getDocAPI(docId);

    await docApi.addRows("Table1", { A: ["x"], B: [1] });

    const forkResult = await docApi.fork();
    const forkId = forkResult.urlId;
    const forkApi = owner.getDocAPI(forkId);

    // Conflicting edits.
    await docApi.updateRows("Table1", { id: [1], A: ["trunk"] });
    await forkApi.updateRows("Table1", { id: [1], A: ["fork"] });

    // Single call with blanket left-wins.
    const result = await callMerge(docId, {
      sourceDocId: forkId,
      resolutions: { cells: [], rows: [], strategy: "left" },
    });
    // Left-wins drops the only source change, so nothing is applied.
    // But the merge completes without error (no unresolved conflicts).
    assert.isFalse(result.applied);

    // Trunk value should be kept (it was never overwritten).
    const rows = await docApi.getRows("Table1");
    assert.equal(rows.A[0], "trunk");
  });

  it("writes merge record and retries are safe", async function() {
    const docId = await owner.newDoc({ name: "merge-retry" }, wsId);
    const docApi = owner.getDocAPI(docId);

    await docApi.addRows("Table1", { A: ["seed"] });

    const forkResult = await docApi.fork();
    const forkId = forkResult.urlId;
    const forkApi = owner.getDocAPI(forkId);

    await forkApi.addRows("Table1", { A: ["fork-data"] });

    // First merge.
    const result1 = await callMerge(docId, { sourceDocId: forkId });
    assert.isTrue(result1.applied);

    const rows1 = await docApi.getRows("Table1");
    assert.lengthOf(rows1.A, 2);

    // "Retry" — call merge again with same source.
    const result2 = await callMerge(docId, { sourceDocId: forkId });
    // Merge record shifts the ancestor — retry sees no new changes.
    assert.isFalse(result2.applied);

    const rows2 = await docApi.getRows("Table1");
    // Exactly 2 rows — no duplicates.
    assert.lengthOf(rows2.A, 2);
    assert.include(rows2.A, "seed");
    assert.include(rows2.A, "fork-data");
  });

  it("handles incremental merge across multiple rounds", async function() {
    const docId = await owner.newDoc({ name: "merge-incremental" }, wsId);
    const docApi = owner.getDocAPI(docId);

    await docApi.addRows("Table1", { A: ["seed"], B: [0] });

    const forkResult = await docApi.fork();
    const forkId = forkResult.urlId;
    const forkApi = owner.getDocAPI(forkId);

    // Round 1: both sides add rows.
    await docApi.addRows("Table1", { A: ["trunk-r1"], B: [10] });
    await forkApi.addRows("Table1", { A: ["fork-r1"], B: [20] });

    const r1 = await callMerge(docId, { sourceDocId: forkId });
    assert.isTrue(r1.applied);

    let rows = await docApi.getRows("Table1");
    assert.lengthOf(rows.A, 3);

    // Round 2: both sides add more rows.
    await docApi.addRows("Table1", { A: ["trunk-r2"], B: [30] });
    await forkApi.addRows("Table1", { A: ["fork-r2"], B: [40] });

    const r2 = await callMerge(docId, { sourceDocId: forkId });
    assert.isTrue(r2.applied);

    rows = await docApi.getRows("Table1");
    assert.lengthOf(rows.A, 5);  // seed + 2*trunk + 2*fork
    assert.include(rows.A, "seed");
    assert.include(rows.A, "trunk-r1");
    assert.include(rows.A, "fork-r1");
    assert.include(rows.A, "trunk-r2");
    assert.include(rows.A, "fork-r2");

    // No duplicates.
    assert.equal(new Set(rows.A).size, 5);
  });

  it("handles delete-update conflict through the endpoint", async function() {
    const docId = await owner.newDoc({ name: "merge-delupd" }, wsId);
    const docApi = owner.getDocAPI(docId);

    await docApi.addRows("Table1", { A: ["alice", "bob"], B: [1, 2] });

    const forkResult = await docApi.fork();
    const forkId = forkResult.urlId;
    const forkApi = owner.getDocAPI(forkId);

    // Target deletes row 2. Fork updates row 2.
    await docApi.removeRows("Table1", [2]);
    await forkApi.updateRows("Table1", { id: [2], A: ["bob-updated"] });

    // First call: should detect row conflict.
    const result1 = await callMerge(docId, { sourceDocId: forkId });
    assert.isFalse(result1.applied);
    assert.lengthOf(result1.conflicts.rows, 1);
    assert.equal(result1.conflicts.rows[0].type, "delete-update");
    assert.equal(result1.conflicts.rows[0].deletedOn, "left");

    // Resolve: keep the row.
    const result2 = await callMerge(docId, {
      sourceDocId: forkId,
      resolutions: {
        cells: [],
        rows: [{ tableId: "Table1", rowId: 2, pick: "keep" }],
      },
    });
    assert.isTrue(result2.applied);

    // Row 2 should be back with fork's updated value.
    const rows = await docApi.getRows("Table1");
    assert.lengthOf(rows.A, 2);
    assert.include(rows.A, "alice");
    assert.include(rows.A, "bob-updated");
  });

  it("rejects merge when source renames a column", async function() {
    const docId = await owner.newDoc({ name: "merge-gate-rename" }, wsId);
    const docApi = owner.getDocAPI(docId);

    await docApi.addRows("Table1", { A: ["data"] });

    const forkResult = await docApi.fork();
    const forkId = forkResult.urlId;
    const forkApi = owner.getDocAPI(forkId);

    // Fork renames column A to AA.
    await forkApi.applyUserActions([
      ["RenameColumn", "Table1", "A", "AA"],
    ]);

    const resp = await callMergeRaw(docId, { sourceDocId: forkId });
    assert.equal(resp.status, 400);
    const body = await resp.json();
    assert.match(body.error, /structural/i);
  });

  it("rejects merge when source deletes a column", async function() {
    const docId = await owner.newDoc({ name: "merge-gate-delcol" }, wsId);
    const docApi = owner.getDocAPI(docId);

    await docApi.addRows("Table1", { A: ["data"], B: [1] });

    const forkResult = await docApi.fork();
    const forkId = forkResult.urlId;
    const forkApi = owner.getDocAPI(forkId);

    // Fork removes column B.
    await forkApi.applyUserActions([
      ["RemoveColumn", "Table1", "B"],
    ]);

    const resp = await callMergeRaw(docId, { sourceDocId: forkId });
    assert.equal(resp.status, 400);
    const body = await resp.json();
    assert.match(body.error, /structural/i);
  });

  it("rejects merge between unrelated documents", async function() {
    // Create two independent docs (no fork relationship).
    const docId1 = await owner.newDoc({ name: "merge-unrelated-1" }, wsId);
    const docId2 = await owner.newDoc({ name: "merge-unrelated-2" }, wsId);

    const resp = await callMergeRaw(docId1, { sourceDocId: docId2 });
    assert.equal(resp.status, 400);
    const body = await resp.json();
    assert.match(body.error, /unrelated/i);
  });

  it("merges source-only column additions", async function() {
    const docId = await owner.newDoc({ name: "merge-addcol" }, wsId);
    const docApi = owner.getDocAPI(docId);

    await docApi.addRows("Table1", { A: ["alice"], B: [100] });

    const forkResult = await docApi.fork();
    const forkId = forkResult.urlId;
    const forkApi = owner.getDocAPI(forkId);

    // Fork adds a new column and populates it.
    await forkApi.applyUserActions([
      ["AddColumn", "Table1", "Notes", { type: "Text" }],
    ]);
    await forkApi.updateRows("Table1", { id: [1], Notes: ["important"] });

    // Trunk makes a data-only edit.
    await docApi.updateRows("Table1", { id: [1], B: [200] });

    // Merge should succeed: source-only column addition is allowed.
    const result = await callMerge(docId, { sourceDocId: forkId });
    assert.isTrue(result.applied);

    // Verify the column exists and data was merged.
    const rows = await docApi.getRows("Table1");
    assert.include(Object.keys(rows), "Notes");
    assert.equal(rows.Notes[0], "important");
    assert.equal(rows.B[0], 200);  // trunk's edit preserved
  });
});
