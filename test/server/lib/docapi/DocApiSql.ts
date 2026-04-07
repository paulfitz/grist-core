/**
 * Tests for SQL endpoint:
 * - GET /docs/{did}/sql
 * - POST /docs/{did}/sql
 *
 * Tests run in multiple server configurations:
 * - Merged server (home + docs in one process)
 * - Separated servers (home + docworker, requires Redis)
 * - Direct to docworker (requires Redis)
 */

import { addAllScenarios, TestContext } from "test/server/lib/docapi/helpers";
import * as testUtils from "test/server/testUtils";

import axios from "axios";
import { assert } from "chai";

describe("DocApiSql", function() {
  this.timeout(30000);
  testUtils.setTmpLogLevel("error");

  addAllScenarios(addSqlTests, "docapi-sql");
});

function addSqlTests(getCtx: () => TestContext) {
  it("GET /docs/{did}/sql is functional", async function() {
    const { homeUrl, docIds, chimpy } = getCtx();
    const query = "select+*+from+Table1+order+by+id";
    const resp = await axios.get(`${homeUrl}/api/docs/${docIds.Timesheets}/sql?q=${query}`, chimpy);
    assert.equal(resp.status, 200);
    assert.deepEqual(resp.data, {
      statement: "select * from Table1 order by id",
      records: [
        {
          fields: {
            id: 1,
            manualSort: 1,
            A: "hello",
            B: "",
            C: "",
            D: null,
            E: "HELLO",
          },
        },
        {
          fields: { id: 2, manualSort: 2, A: "", B: "world", C: "", D: null, E: "" },
        },
        {
          fields: { id: 3, manualSort: 3, A: "", B: "", C: "", D: null, E: "" },
        },
        {
          fields: { id: 4, manualSort: 4, A: "", B: "", C: "", D: null, E: "" },
        },
      ],
    });
  });

  it("POST /docs/{did}/sql is functional", async function() {
    const { homeUrl, docIds, chimpy } = getCtx();
    let resp = await axios.post(
      `${homeUrl}/api/docs/${docIds.Timesheets}/sql`,
      { sql: "select A from Table1 where id = ?", args: [1] },
      chimpy);
    assert.equal(resp.status, 200);
    assert.deepEqual(resp.data.records, [{
      fields: {
        A: "hello",
      },
    }]);

    resp = await axios.post(
      `${homeUrl}/api/docs/${docIds.Timesheets}/sql`,
      { nosql: "select A from Table1 where id = ?", args: [1] },
      chimpy);
    assert.equal(resp.status, 400);
    assert.deepEqual(resp.data, {
      error: "Invalid payload",
      details: { userError: "Error: body.sql is missing" },
    });
  });

  it("POST /docs/{did}/sql has access control", async function() {
    const { homeUrl, docIds, chimpy, kiwi, flushAuth } = getCtx();
    // Check non-viewer doesn't have access.
    const url = `${homeUrl}/api/docs/${docIds.Timesheets}/sql`;
    const query = { sql: "select A from Table1 where id = ?", args: [1] };
    let resp = await axios.post(url, query, kiwi);
    assert.equal(resp.status, 403);
    assert.deepEqual(resp.data, {
      error: "No view access",
    });

    try {
      // Check a viewer would have access.
      const delta = {
        users: { "kiwi@getgrist.com": "viewers" },
      };
      await axios.patch(`${homeUrl}/api/docs/${docIds.Timesheets}/access`, { delta }, chimpy);
      await flushAuth();
      resp = await axios.post(url, query, kiwi);
      assert.equal(resp.status, 200);

      // Check a viewer would not have access if there is some private material.
      await axios.post(
        `${homeUrl}/api/docs/${docIds.Timesheets}/apply`, [
          ["AddTable", "TablePrivate", [{ id: "A", type: "Int" }]],
          ["AddRecord", "_grist_ACLResources", -1, { tableId: "TablePrivate", colIds: "*" }],
          ["AddRecord", "_grist_ACLRules", null, {
            resource: -1, aclFormula: "", permissionsText: "none",
          }],
        ], chimpy);
      resp = await axios.post(url, query, kiwi);
      assert.equal(resp.status, 403);
    } finally {
      // Remove extra viewer; remove extra table.
      const delta = {
        users: { "kiwi@getgrist.com": null },
      };
      await axios.patch(`${homeUrl}/api/docs/${docIds.Timesheets}/access`, { delta }, chimpy);
      await flushAuth();
      await axios.post(
        `${homeUrl}/api/docs/${docIds.Timesheets}/apply`, [
          ["RemoveTable", "TablePrivate"],
        ], chimpy);
    }
  });

  it("POST /docs/{did}/sql accepts only selects", async function() {
    const { homeUrl, docIds, chimpy } = getCtx();
    async function check(accept: boolean, sql: string, ...args: any[]) {
      const resp = await axios.post(
        `${homeUrl}/api/docs/${docIds.Timesheets}/sql`,
        { sql, args },
        chimpy);
      if (accept) {
        assert.equal(resp.status, 200);
      } else {
        assert.equal(resp.status, 400);
      }
      return resp.data;
    }
    await check(true, "select * from Table1");
    await check(true, "  SeLeCT * from Table1");
    await check(true, "with results as (select 1) select * from results");

    // rejected quickly since no select
    await check(false, "delete from Table1");
    await check(false, "");

    // rejected because deletes/updates/... can't be nested within a select
    await check(false, "delete from Table1 where id in (select id from Table1) and 'selecty' = 'selecty'");
    await check(false, "update Table1 set A = ? where 'selecty' = 'selecty'", "test");
    await check(false, "pragma data_store_directory = 'selecty'");
    await check(false, "create table selecty(x, y)");
    await check(false, "attach database 'selecty' AS test");

    // rejected because ";" can't be nested
    await check(false, "select * from Table1; delete from Table1");

    // The wrapping "select * from (...)" prevents SQL injection within a
    // single statement. Multi-statement injection (breaking out via ";")
    // is handled differently per backend:
    // - SQLite: node-sqlite3 only executes the first statement (200, DELETE discarded)
    // - Postgres: extended query protocol rejects multiple statements (400)
    // Either way, the DELETE must not execute.
    const injectionSql = "select * from Table1); delete from Table1 where id in (select id from Table1";
    const injResp = await axios.post(
      `${homeUrl}/api/docs/${docIds.Timesheets}/sql`,
      { sql: injectionSql },
      chimpy);
    assert.include([200, 400], injResp.status);
    const { records } = await check(true, "select * from Table1");
    // Double-check the deletion didn't happen.
    assert.lengthOf(records, 4);
  });

  it("POST /docs/{did}/sql timeout is effective", async function() {
    const { homeUrl, docIds, chimpy } = getCtx();
    // Use a recursive CTE that's slow on both SQLite and Postgres.
    // Postgres doesn't support LIMIT inside recursive CTEs, so use
    // a WHERE condition to bound recursion and count(*) to force
    // full evaluation.
    const slowQuery = "WITH RECURSIVE r(i) AS (VALUES(0) " +
      "UNION ALL SELECT i + 1 FROM r WHERE i < 1000000) " +
      "SELECT count(*) FROM r";
    const resp = await axios.post(
      `${homeUrl}/api/docs/${docIds.Timesheets}/sql`,
      { sql: slowQuery, timeout: 10 },
      chimpy);
    assert.equal(resp.status, 400);
    assert.match(resp.data.error, /database interrupt|cancel/);
  });
}
