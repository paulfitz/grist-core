import {
  ActionSummary, createEmptyActionSummary, createEmptyTableDelta, TableDelta,
} from "app/common/ActionSummary";
import {
  applyResolutions, ColumnTypeMap, detectConflicts,
  isMergeConflictsEmpty, MergeConflicts, remapCollisions,
} from "app/common/MergeUtils";

import { assert } from "chai";

/** Build a TableDelta with defaults filled in. */
function td(partial: Partial<TableDelta> = {}): TableDelta {
  return { ...createEmptyTableDelta(), ...partial };
}

/** Build an ActionSummary from a partial tableDeltas map. */
function summary(tableDeltas: { [tableId: string]: Partial<TableDelta> }): ActionSummary {
  const result = createEmptyActionSummary();
  for (const [tableId, partial] of Object.entries(tableDeltas)) {
    result.tableDeltas[tableId] = td(partial);
  }
  return result;
}

/** Shorthand for a cell that was added (didn't exist → has value). */
function added(value: any): [null, [any]] { return [null, [value]]; }

/** Shorthand for a cell that was removed (had value → doesn't exist). */
function removed(value: any): [[any], null] { return [[value], null]; }

/** Shorthand for a cell that was updated. */
function updated(from: any, to: any): [[any], [any]] { return [[from], [to]]; }

describe("MergeUtils", function() {
  // ==================== remapCollisions ====================

  describe("remapCollisions", function() {
    it("returns original summary when no collisions", function() {
      const left = summary({
        Contacts: { addRows: [2, 3] },
      });
      const right = summary({
        Contacts: { addRows: [4, 5] },
      });
      const targetRowIds = new Map([["Contacts", new Set([1, 2, 3])]]);
      const colTypes: ColumnTypeMap = {};

      const { remapped, remap } = remapCollisions(right, left, targetRowIds, colTypes);

      // No collisions — should return the exact same object.
      assert.strictEqual(remapped, right);
      assert.deepEqual(remap, {});
    });

    it("remaps colliding addRows to negative placeholders", function() {
      const left = summary({
        Contacts: { addRows: [2, 3] },
      });
      const right = summary({
        Contacts: {
          addRows: [2, 3, 4],
          columnDeltas: {
            Name: {
              2: added("Alice"),
              3: added("Bob"),
              4: added("Carol"),
            },
          },
        },
      });
      // Target has rows 1, 2, 3 (1 from ancestor, 2–3 from left adds).
      const targetRowIds = new Map([["Contacts", new Set([1, 2, 3])]]);
      const colTypes: ColumnTypeMap = {};

      const { remapped, remap } = remapCollisions(right, left, targetRowIds, colTypes);

      // Rows 2 and 3 collide; row 4 does not.
      assert.isTrue(remap.Contacts.has(2));
      assert.isTrue(remap.Contacts.has(3));
      assert.isFalse(remap.Contacts.has(4));

      const p2 = remap.Contacts.get(2)!;
      const p3 = remap.Contacts.get(3)!;
      assert.isBelow(p2, 0);
      assert.isBelow(p3, 0);
      assert.notEqual(p2, p3);

      // Check the remapped summary.
      const rtd = remapped.tableDeltas.Contacts;
      assert.includeMembers(rtd.addRows, [p2, p3, 4]);
      assert.notInclude(rtd.addRows, 2);
      assert.notInclude(rtd.addRows, 3);

      // Column deltas re-keyed.
      assert.deepEqual(rtd.columnDeltas.Name[p2], added("Alice"));
      assert.deepEqual(rtd.columnDeltas.Name[p3], added("Bob"));
      assert.deepEqual(rtd.columnDeltas.Name[4], added("Carol"));
      assert.isUndefined(rtd.columnDeltas.Name[2]);
      assert.isUndefined(rtd.columnDeltas.Name[3]);
    });

    it("rewrites Ref values in column deltas for remapped rows", function() {
      // Source adds People row 4 (collides) and Projects row 2 (collides).
      // Projects.lead is Ref:People, pointing to People row 4.
      const right = summary({
        People: {
          addRows: [4],
          columnDeltas: { Name: { 4: added("Bob") } },
        },
        Projects: {
          addRows: [2],
          columnDeltas: { lead: { 2: added(4) } },  // Ref:People → row 4
        },
      });
      const left = summary({});
      const targetRowIds = new Map([
        ["People", new Set([1, 2, 3, 4])],    // People row 4 exists in target
        ["Projects", new Set([1, 2])],          // Projects row 2 exists in target
      ]);
      const colTypes: ColumnTypeMap = {
        Projects: { lead: "Ref:People" },
      };

      const { remapped, remap } = remapCollisions(right, left, targetRowIds, colTypes);

      const peopleP = remap.People.get(4)!;
      const projectsP = remap.Projects.get(2)!;
      assert.isBelow(peopleP, 0);
      assert.isBelow(projectsP, 0);

      // The Projects.lead cell value should now point to the People placeholder.
      const leadDelta = remapped.tableDeltas.Projects.columnDeltas.lead[projectsP];
      assert.deepEqual(leadDelta, added(peopleP));
    });

    it("rewrites RefList values", function() {
      const right = summary({
        People: {
          addRows: [4, 5],
          columnDeltas: { Name: { 4: added("A"), 5: added("B") } },
        },
        Groups: {
          addRows: [2],
          columnDeltas: {
            members: { 2: added(["L", 4, 5, 1]) },  // RefList:People
          },
        },
      });
      const left = summary({});
      const targetRowIds = new Map([
        ["People", new Set([1, 2, 3, 4, 5])],
        ["Groups", new Set([1, 2])],
      ]);
      const colTypes: ColumnTypeMap = {
        Groups: { members: "RefList:People" },
      };

      const { remapped, remap } = remapCollisions(right, left, targetRowIds, colTypes);

      const p4 = remap.People.get(4)!;
      const p5 = remap.People.get(5)!;
      const gp2 = remap.Groups.get(2)!;

      const membersDelta = remapped.tableDeltas.Groups.columnDeltas.members[gp2];
      // The RefList should have remapped IDs for 4 and 5, but 1 stays (not remapped).
      assert.deepEqual(membersDelta, added(["L", p4, p5, 1]));
    });

    it("does not remap rows that only exist in the ancestor (target removed them)", function() {
      // Ancestor had row 5. Target deleted it. Source adds row 5.
      // Since target's *current* rows don't include 5, no collision.
      const right = summary({
        Contacts: {
          addRows: [5],
          columnDeltas: { Name: { 5: added("New") } },
        },
      });
      const left = summary({});
      const targetRowIds = new Map([["Contacts", new Set([1, 2, 3])]]);  // No row 5
      const colTypes: ColumnTypeMap = {};

      const { remapped, remap } = remapCollisions(right, left, targetRowIds, colTypes);

      assert.strictEqual(remapped, right);  // No collisions.
      assert.deepEqual(remap, {});
    });

    it("handles non-colliding row referencing a colliding row", function() {
      // People row 4 collides. People row 10 does not collide.
      // Projects row 1 (existing, updated) has Ref:People pointing to 10.
      // Projects row 5 (new, no collision) has Ref:People pointing to 4 (colliding).
      const right = summary({
        People: {
          addRows: [4, 10],
          columnDeltas: { Name: { 4: added("Bob"), 10: added("Zara") } },
        },
        Projects: {
          addRows: [5],
          updateRows: [1],
          columnDeltas: {
            lead: {
              1: updated(1, 10),  // update existing row, now points to People 10
              5: added(4),        // new row, points to People 4 (colliding)
            },
          },
        },
      });
      const left = summary({});
      const targetRowIds = new Map([
        ["People", new Set([1, 2, 3, 4])],     // People 4 collides, 10 does not
        ["Projects", new Set([1, 2])],           // Projects 5 does not collide
      ]);
      const colTypes: ColumnTypeMap = {
        Projects: { lead: "Ref:People" },
      };

      const { remapped, remap } = remapCollisions(right, left, targetRowIds, colTypes);

      const p4 = remap.People.get(4)!;
      assert.isBelow(p4, 0);
      assert.isFalse((remap.People || new Map()).has(10));  // 10 doesn't collide

      // Projects row 5 didn't collide — stays as 5 in addRows.
      assert.include(remapped.tableDeltas.Projects.addRows, 5);

      // Projects.lead for row 5: was pointing to People 4, should now point to placeholder.
      assert.deepEqual(remapped.tableDeltas.Projects.columnDeltas.lead[5], added(p4));

      // Projects.lead for row 1 (update): was pointing to People 10, which wasn't remapped.
      assert.deepEqual(remapped.tableDeltas.Projects.columnDeltas.lead[1], updated(1, 10));
    });

    it("skips metadata tables", function() {
      const right = summary({
        _grist_Tables_column: { addRows: [7] },
        Contacts: { addRows: [2] },
      });
      const left = summary({});
      const targetRowIds = new Map([["Contacts", new Set([1, 2])]]);
      const colTypes: ColumnTypeMap = {};

      const { remapped, remap } = remapCollisions(right, left, targetRowIds, colTypes);

      // Metadata table should be untouched.
      assert.deepEqual(remapped.tableDeltas._grist_Tables_column, right.tableDeltas._grist_Tables_column);
      // User table should be remapped.
      assert.isTrue(remap.Contacts?.has(2));
    });
  });

  // ==================== detectConflicts ====================

  describe("detectConflicts", function() {
    it("returns empty when no overlapping changes", function() {
      const left = summary({
        Contacts: {
          updateRows: [1],
          columnDeltas: { Email: { 1: updated("old@x", "new@x") } },
        },
      });
      const right = summary({
        Contacts: {
          updateRows: [2],
          columnDeltas: { Email: { 2: updated("a@x", "b@x") } },
        },
      });

      const { conflicts } = detectConflicts(left, right);
      assert.isTrue(isMergeConflictsEmpty(conflicts));
    });

    it("returns empty when changes are to different columns of the same row", function() {
      const left = summary({
        Contacts: {
          updateRows: [1],
          columnDeltas: { Email: { 1: updated("old@x", "new@x") } },
        },
      });
      const right = summary({
        Contacts: {
          updateRows: [1],
          columnDeltas: { Phone: { 1: updated("111", "222") } },
        },
      });

      const { conflicts } = detectConflicts(left, right);
      assert.isTrue(isMergeConflictsEmpty(conflicts));
    });

    it("detects cell conflict when both sides update the same cell", function() {
      const left = summary({
        Projects: {
          updateRows: [1],
          columnDeltas: { Status: { 1: updated("Open", "On Hold") } },
        },
      });
      const right = summary({
        Projects: {
          updateRows: [1],
          columnDeltas: { Status: { 1: updated("Open", "Completed") } },
        },
      });

      const { conflicts } = detectConflicts(left, right);
      assert.lengthOf(conflicts.cells, 1);
      assert.lengthOf(conflicts.rows, 0);

      const c = conflicts.cells[0];
      assert.equal(c.tableId, "Projects");
      assert.equal(c.colId, "Status");
      assert.equal(c.rowId, 1);
      assert.equal(c.ancestor, "Open");
      assert.equal(c.left, "On Hold");
      assert.equal(c.right, "Completed");
    });

    it("treats convergent edits as non-conflicting (primitives)", function() {
      const left = summary({
        T: {
          updateRows: [1],
          columnDeltas: { Budget: { 1: updated(30000, 35000) } },
        },
      });
      const right = summary({
        T: {
          updateRows: [1],
          columnDeltas: { Budget: { 1: updated(30000, 35000) } },
        },
      });

      const { conflicts } = detectConflicts(left, right);
      assert.isTrue(isMergeConflictsEmpty(conflicts));
    });

    it("treats convergent edits as non-conflicting (deep equality for encoded values)", function() {
      // Both sides set a RefList to the same value.
      const refList = ["L", 1, 2, 3];
      const left = summary({
        T: {
          updateRows: [1],
          columnDeltas: { tags: { 1: updated(null, refList) } },
        },
      });
      const right = summary({
        T: {
          updateRows: [1],
          columnDeltas: { tags: { 1: updated(null, refList) } },
        },
      });

      const { conflicts } = detectConflicts(left, right);
      assert.isTrue(isMergeConflictsEmpty(conflicts));
    });

    it("detects conflict even when ancestor values differ (net-effect limitation)", function() {
      // This tests the acknowledged limitation: summaries show net effects.
      // Left: X→Y. Right: X→Z (even if right went X→Y→Z, we only see X→Z).
      const left = summary({
        T: {
          updateRows: [1],
          columnDeltas: { Status: { 1: updated("X", "Y") } },
        },
      });
      const right = summary({
        T: {
          updateRows: [1],
          columnDeltas: { Status: { 1: updated("X", "Z") } },
        },
      });

      const { conflicts } = detectConflicts(left, right);
      assert.lengthOf(conflicts.cells, 1);
      assert.equal(conflicts.cells[0].left, "Y");
      assert.equal(conflicts.cells[0].right, "Z");
    });

    it("detects delete-update conflict (left deletes, right updates)", function() {
      const left = summary({
        Leads: {
          removeRows: [2],
          columnDeltas: {
            Name: { 2: removed("Evan") },
            Phone: { 2: removed("555-0002") },
          },
        },
      });
      const right = summary({
        Leads: {
          updateRows: [2],
          columnDeltas: {
            Phone: { 2: updated("555-0002", "555-9999") },
          },
        },
      });

      const { conflicts } = detectConflicts(left, right);
      assert.lengthOf(conflicts.cells, 0);
      assert.lengthOf(conflicts.rows, 1);

      const rc = conflicts.rows[0];
      assert.equal(rc.tableId, "Leads");
      assert.equal(rc.rowId, 2);
      assert.equal(rc.type, "delete-update");
      assert.equal(rc.deletedOn, "left");
    });

    it("detects delete-update conflict (right deletes, left updates)", function() {
      const left = summary({
        Leads: {
          updateRows: [2],
          columnDeltas: { Phone: { 2: updated("555-0002", "555-9999") } },
        },
      });
      const right = summary({
        Leads: {
          removeRows: [2],
          columnDeltas: { Phone: { 2: removed("555-0002") } },
        },
      });

      const { conflicts } = detectConflicts(left, right);
      assert.lengthOf(conflicts.rows, 1);
      assert.equal(conflicts.rows[0].deletedOn, "right");
    });

    it("detects multiple conflicts across tables", function() {
      const left = summary({
        People: {
          updateRows: [1, 2],
          columnDeltas: {
            Name: { 1: updated("A", "B"), 2: updated("C", "D") },
          },
        },
        Projects: {
          updateRows: [1],
          columnDeltas: { Status: { 1: updated("Open", "Closed") } },
        },
      });
      const right = summary({
        People: {
          updateRows: [1, 2],
          columnDeltas: {
            Name: { 1: updated("A", "X"), 2: updated("C", "Y") },
          },
        },
        Projects: {
          updateRows: [1],
          columnDeltas: { Status: { 1: updated("Open", "Paused") } },
        },
      });

      const { conflicts } = detectConflicts(left, right);
      assert.lengthOf(conflicts.cells, 3);

      const byTable = (t: string) => conflicts.cells.filter((c: any) => c.tableId === t);
      assert.lengthOf(byTable("People"), 2);
      assert.lengthOf(byTable("Projects"), 1);
    });

    it("ignores metadata tables", function() {
      const left = summary({
        _grist_Tables_column: {
          updateRows: [5],
          columnDeltas: { label: { 5: updated("Old", "New") } },
        },
      });
      const right = summary({
        _grist_Tables_column: {
          updateRows: [5],
          columnDeltas: { label: { 5: updated("Old", "Different") } },
        },
      });

      const { conflicts } = detectConflicts(left, right);
      assert.isTrue(isMergeConflictsEmpty(conflicts));
    });

    it("handles remap + conflict interaction correctly", function() {
      // After remap, source's addRow has placeholder -1. Target has addRow 4.
      // Both update existing row 1. These should conflict on the cell level,
      // not be confused by the addRows.
      const left = summary({
        T: {
          addRows: [4],
          updateRows: [1],
          columnDeltas: {
            Name: { 1: updated("X", "Y"), 4: added("New-left") },
          },
        },
      });
      const right = summary({
        T: {
          addRows: [-1],  // already remapped
          updateRows: [1],
          columnDeltas: {
            Name: { 1: updated("X", "Z"), [-1]: added("New-right") },
          },
        },
      });

      const { conflicts } = detectConflicts(left, right);
      // Only row 1 Name is a conflict. The addRows don't overlap.
      assert.lengthOf(conflicts.cells, 1);
      assert.equal(conflicts.cells[0].rowId, 1);
      assert.lengthOf(conflicts.rows, 0);
    });
  });

  // ==================== applyResolutions ====================

  describe("applyResolutions", function() {
    it("drops conflicting cells when resolved as 'left'", function() {
      const right = summary({
        T: {
          updateRows: [1, 2],
          columnDeltas: {
            Status: { 1: updated("Open", "Completed") },
            Budget: { 2: updated(100, 200) },
          },
        },
      });
      const conflicts: MergeConflicts = {
        cells: [{
          tableId: "T", colId: "Status", rowId: 1,
          ancestor: "Open", left: "On Hold", right: "Completed",
        }],
        rows: [],
      };
      const resolutions = {
        cells: [{ tableId: "T", colId: "Status", rowId: 1, pick: "left" as const }],
        rows: [],
      };

      const resolved = applyResolutions(right, conflicts, resolutions);

      // Status for row 1 should be dropped (left wins).
      assert.isUndefined(resolved.tableDeltas.T.columnDeltas.Status);
      // Budget for row 2 should remain (not conflicting).
      assert.deepEqual(resolved.tableDeltas.T.columnDeltas.Budget[2], updated(100, 200));
    });

    it("keeps conflicting cells when resolved as 'right'", function() {
      const right = summary({
        T: {
          updateRows: [1],
          columnDeltas: {
            Status: { 1: updated("Open", "Completed") },
          },
        },
      });
      const conflicts: MergeConflicts = {
        cells: [{
          tableId: "T", colId: "Status", rowId: 1,
          ancestor: "Open", left: "On Hold", right: "Completed",
        }],
        rows: [],
      };
      const resolutions = {
        cells: [{ tableId: "T", colId: "Status", rowId: 1, pick: "right" as const }],
        rows: [],
      };

      const resolved = applyResolutions(right, conflicts, resolutions);
      assert.deepEqual(resolved.tableDeltas.T.columnDeltas.Status[1], updated("Open", "Completed"));
    });

    it("uses blanket strategy when no per-cell resolution", function() {
      const right = summary({
        T: {
          updateRows: [1, 2],
          columnDeltas: {
            A: { 1: updated("x", "y") },
            B: { 2: updated("a", "b") },
          },
        },
      });
      const conflicts: MergeConflicts = {
        cells: [
          { tableId: "T", colId: "A", rowId: 1, ancestor: "x", left: "L", right: "y" },
          { tableId: "T", colId: "B", rowId: 2, ancestor: "a", left: "L2", right: "b" },
        ],
        rows: [],
      };
      const resolutions = { cells: [], rows: [], strategy: "left" as const };

      const resolved = applyResolutions(right, conflicts, resolutions);
      // Both conflicting cells dropped.
      assert.deepEqual(resolved.tableDeltas.T.columnDeltas, {});
    });

    it("per-cell resolution overrides blanket strategy", function() {
      const right = summary({
        T: {
          updateRows: [1, 2],
          columnDeltas: {
            A: { 1: updated("x", "y") },
            B: { 2: updated("a", "b") },
          },
        },
      });
      const conflicts: MergeConflicts = {
        cells: [
          { tableId: "T", colId: "A", rowId: 1, ancestor: "x", left: "L", right: "y" },
          { tableId: "T", colId: "B", rowId: 2, ancestor: "a", left: "L2", right: "b" },
        ],
        rows: [],
      };
      const resolutions = {
        cells: [{ tableId: "T", colId: "A", rowId: 1, pick: "right" as const }],
        rows: [],
        strategy: "left" as const,  // fallback for B
      };

      const resolved = applyResolutions(right, conflicts, resolutions);
      // A kept (explicit right), B dropped (blanket left).
      assert.deepEqual(resolved.tableDeltas.T.columnDeltas.A[1], updated("x", "y"));
      assert.isUndefined(resolved.tableDeltas.T.columnDeltas.B);
    });

    it("handles delete-update row conflict resolved as 'delete'", function() {
      const right = summary({
        Leads: {
          updateRows: [2],
          columnDeltas: {
            Phone: { 2: updated("555-0002", "555-9999") },
            Score: { 2: updated(7, 8) },
          },
        },
      });
      const conflicts: MergeConflicts = {
        cells: [],
        rows: [{ tableId: "Leads", rowId: 2, type: "delete-update", deletedOn: "left" }],
      };
      const resolutions = {
        cells: [],
        rows: [{ tableId: "Leads", rowId: 2, pick: "delete" as const }],
      };

      const resolved = applyResolutions(right, conflicts, resolutions);
      // Source's updates for row 2 should be removed.
      assert.notInclude(resolved.tableDeltas.Leads.updateRows, 2);
      assert.deepEqual(resolved.tableDeltas.Leads.columnDeltas, {});
    });
  });

  // ==================== Multi-round divergence ====================

  describe("repeated merges with diverging rowIds", function() {
    /**
     * Simulates the full merge-apply cycle without a server.
     *
     * Given the current target rowIds and a resolved source summary,
     * "apply" the adds: for colliding rows (negative placeholders),
     * assign the next available ID. For non-colliding rows, keep the
     * original ID. Returns the updated targetRowIds and the final
     * remap (placeholder → assigned ID).
     */
    function simulateApply(
      targetRowIds: Set<number>,
      resolvedTd: TableDelta,
      remap: Map<number, number> | undefined,
    ): { newTargetRowIds: Set<number>; finalRemap: Map<number, number> } {
      const newIds = new Set(targetRowIds);
      let nextId = Math.max(0, ...targetRowIds) + 1;
      const finalRemap = new Map<number, number>();

      for (const rowId of resolvedTd.addRows) {
        if (rowId < 0) {
          // Colliding row — assign next available.
          finalRemap.set(rowId, nextId);
          newIds.add(nextId);
          nextId++;
        } else {
          // Non-colliding — use the original ID.
          newIds.add(rowId);
          finalRemap.set(rowId, rowId);
        }
      }

      return { newTargetRowIds: newIds, finalRemap };
    }

    it("handles 5 rounds of both sides adding rows to the same table", function() {
      const colTypes: ColumnTypeMap = {};

      // Ancestor: People rows [1, 2, 3].
      let targetRowIds = new Set([1, 2, 3]);
      let sourceMax = 3;  // Source's next rowId will be sourceMax + 1.
      let targetMax = 3;  // Target's next rowId will be targetMax + 1.

      // Track all names that end up in the target, to verify no data loss.
      const allTargetNames: string[] = ["Ancestor-1", "Ancestor-2", "Ancestor-3"];

      for (let round = 1; round <= 5; round++) {
        // Each round, both sides add 3 rows.
        const targetNewIds = [targetMax + 1, targetMax + 2, targetMax + 3];
        const sourceNewIds = [sourceMax + 1, sourceMax + 2, sourceMax + 3];
        targetMax += 3;
        sourceMax += 3;

        // Simulate target adding its rows.
        const targetNames: { [id: number]: string } = {};
        for (const id of targetNewIds) {
          targetRowIds.add(id);
          const name = `Target-R${round}-${id}`;
          targetNames[id] = name;
          allTargetNames.push(name);
        }

        // Build left summary (target's adds this round).
        const leftColumnDeltas: { [id: number]: [null, [string]] } = {};
        for (const id of targetNewIds) {
          leftColumnDeltas[id] = added(targetNames[id]);
        }
        const left = summary({
          People: {
            addRows: targetNewIds,
            columnDeltas: { Name: leftColumnDeltas },
          },
        });

        // Build right summary (source's adds this round).
        const sourceNames: { [id: number]: string } = {};
        const rightColumnDeltas: { [id: number]: [null, [string]] } = {};
        for (const id of sourceNewIds) {
          const name = `Source-R${round}-${id}`;
          sourceNames[id] = name;
          rightColumnDeltas[id] = added(name);
        }
        const right = summary({
          People: {
            addRows: sourceNewIds,
            columnDeltas: { Name: rightColumnDeltas },
          },
        });

        // Step 3: Remap.
        const { remapped, remap } = remapCollisions(right, left, new Map([["People", targetRowIds]]), colTypes);

        // Step 4: Detect conflicts.
        const { conflicts } = detectConflicts(left, remapped);

        // Added rows should never conflict with each other (different IDs after remap).
        assert.isTrue(isMergeConflictsEmpty(conflicts),
          `Round ${round}: expected no conflicts but got ${JSON.stringify(conflicts)}`);

        // Step 7 (simulated): Apply the remapped adds.
        const remappedTd = remapped.tableDeltas.People;
        const { newTargetRowIds, finalRemap } = simulateApply(
          targetRowIds, remappedTd, remap.People,
        );

        // Verify: every source row got a unique ID that doesn't clash with any target row.
        for (const sourceId of sourceNewIds) {
          const placeholder = remap.People?.get(sourceId);
          if (placeholder !== undefined) {
            // This row was remapped. Check the final ID.
            const finalId = finalRemap.get(placeholder)!;
            assert.isAbove(finalId, 0, `Round ${round}: final ID should be positive`);
            assert.isFalse(targetRowIds.has(finalId),
              `Round ${round}: final ID ${finalId} should not have existed in target before apply`);
          } else {
            // Not remapped — original ID shouldn't be in the target.
            assert.isFalse(targetRowIds.has(sourceId),
              `Round ${round}: non-remapped ID ${sourceId} shouldn't be in target`);
          }
        }

        // Record the source names.
        for (const sourceId of sourceNewIds) {
          allTargetNames.push(sourceNames[sourceId]);
        }

        // Update target state for next round.
        targetRowIds = newTargetRowIds;
        // After merge, target's max is the highest ID in the set.
        targetMax = Math.max(...targetRowIds);
      }

      // After 5 rounds: ancestor had 3 rows, each round added 3+3 = 6.
      // Total: 3 + 5*6 = 33 rows.
      assert.equal(targetRowIds.size, 33);
      assert.equal(allTargetNames.length, 33);

      // Every name is unique — no data was lost or duplicated.
      assert.equal(new Set(allTargetNames).size, 33);
    });

    it("handles divergence where source's new IDs collide with previously-merged rows", function() {
      // This is the tricky case: after round 1, the target has rows
      // that were assigned to the source via remap. In round 2, the
      // source allocates IDs that collide with those remapped rows.
      const colTypes: ColumnTypeMap = {};

      // Ancestor: rows [1, 2].
      let targetRowIds = new Set([1, 2]);

      // Round 1: target adds [3, 4], source adds [3, 4].
      {
        const left = summary({
          T: {
            addRows: [3, 4],
            columnDeltas: { V: { 3: added("T3"), 4: added("T4") } },
          },
        });
        const right = summary({
          T: {
            addRows: [3, 4],
            columnDeltas: { V: { 3: added("S3"), 4: added("S4") } },
          },
        });

        // Target now has [1, 2, 3, 4].
        targetRowIds.add(3);
        targetRowIds.add(4);

        const { remapped, remap } = remapCollisions(
          right, left, new Map([["T", targetRowIds]]), colTypes,
        );

        // Source 3 and 4 both collide.
        assert.isTrue(remap.T?.has(3));
        assert.isTrue(remap.T?.has(4));
        assert.isBelow(remap.T.get(3)!, 0);
        assert.isBelow(remap.T.get(4)!, 0);

        const { conflicts } = detectConflicts(left, remapped);
        assert.isTrue(isMergeConflictsEmpty(conflicts));

        // Simulate apply: placeholders get IDs 5 and 6.
        const { newTargetRowIds } = simulateApply(targetRowIds, remapped.tableDeltas.T, remap.T);
        targetRowIds = newTargetRowIds;

        // Target now has [1, 2, 3, 4, 5, 6].
        assert.deepEqual([...targetRowIds].sort((a, b) => a - b), [1, 2, 3, 4, 5, 6]);
      }

      // Round 2: target adds [7, 8], source adds [5, 6].
      // Source's max after round 1 was 4, so it allocates 5, 6.
      // But target has 5 and 6 from the round-1 remap!
      {
        const left = summary({
          T: {
            addRows: [7, 8],
            columnDeltas: { V: { 7: added("T7"), 8: added("T8") } },
          },
        });
        const right = summary({
          T: {
            addRows: [5, 6],
            columnDeltas: { V: { 5: added("S5"), 6: added("S6") } },
          },
        });

        targetRowIds.add(7);
        targetRowIds.add(8);

        const { remapped, remap } = remapCollisions(
          right, left, new Map([["T", targetRowIds]]), colTypes,
        );

        // Source's 5 and 6 should collide (target has them from round 1 merge).
        assert.isTrue(remap.T?.has(5), "Source row 5 should collide with previously-merged row");
        assert.isTrue(remap.T?.has(6), "Source row 6 should collide with previously-merged row");

        const { conflicts } = detectConflicts(left, remapped);
        assert.isTrue(isMergeConflictsEmpty(conflicts));

        // Simulate apply: placeholders get IDs 9 and 10.
        const { newTargetRowIds } = simulateApply(targetRowIds, remapped.tableDeltas.T, remap.T);
        targetRowIds = newTargetRowIds;

        assert.deepEqual([...targetRowIds].sort((a, b) => a - b), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
      }

      // Round 3: source adds [7, 8] — these collide with target's round-2 adds.
      // Target's max is now 10, so target adds [11, 12].
      {
        const left = summary({
          T: {
            addRows: [11, 12],
            columnDeltas: { V: { 11: added("T11"), 12: added("T12") } },
          },
        });
        const right = summary({
          T: {
            addRows: [7, 8],  // source's max was 6, now allocates 7, 8
            columnDeltas: { V: { 7: added("S7"), 8: added("S8") } },
          },
        });

        targetRowIds.add(11);
        targetRowIds.add(12);

        const { remapped, remap } = remapCollisions(
          right, left, new Map([["T", targetRowIds]]), colTypes,
        );

        assert.isTrue(remap.T?.has(7), "Source row 7 should collide");
        assert.isTrue(remap.T?.has(8), "Source row 8 should collide");

        const { conflicts } = detectConflicts(left, remapped);
        assert.isTrue(isMergeConflictsEmpty(conflicts));

        const { newTargetRowIds } = simulateApply(targetRowIds, remapped.tableDeltas.T, remap.T);
        targetRowIds = newTargetRowIds;

        // 2 original + 3 rounds × 4 adds (2 from target + 2 from source) = 14 rows.
        assert.equal(targetRowIds.size, 14);
      }
    });

    it("handles diverging Ref columns across rounds", function() {
      // People and Projects tables. Projects.lead is Ref:People.
      // Each round, both sides add a person and a project pointing to them.
      const colTypes: ColumnTypeMap = {
        Projects: { lead: "Ref:People" },
      };

      let peopleIds = new Set([1]);
      let projectIds = new Set([1]);
      let sourcePeopleMax = 1;
      let sourceProjectMax = 1;

      for (let round = 1; round <= 3; round++) {
        // Target adds a person and project.
        const tPersonId = Math.max(...peopleIds) + 1;
        const tProjectId = Math.max(...projectIds) + 1;
        peopleIds.add(tPersonId);
        projectIds.add(tProjectId);

        // Source adds a person and project.
        const sPersonId = sourcePeopleMax + 1;
        const sProjectId = sourceProjectMax + 1;
        sourcePeopleMax = sPersonId;
        sourceProjectMax = sProjectId;

        const left = summary({
          People: {
            addRows: [tPersonId],
            columnDeltas: { Name: { [tPersonId]: added(`TP-${round}`) } },
          },
          Projects: {
            addRows: [tProjectId],
            columnDeltas: {
              title: { [tProjectId]: added(`TProj-${round}`) },
              lead: { [tProjectId]: added(tPersonId) },
            },
          },
        });

        const right = summary({
          People: {
            addRows: [sPersonId],
            columnDeltas: { Name: { [sPersonId]: added(`SP-${round}`) } },
          },
          Projects: {
            addRows: [sProjectId],
            columnDeltas: {
              title: { [sProjectId]: added(`SProj-${round}`) },
              lead: { [sProjectId]: added(sPersonId) },
            },
          },
        });

        const targetRowIds = new Map([
          ["People", peopleIds],
          ["Projects", projectIds],
        ]);

        const { remapped, remap } = remapCollisions(right, left, targetRowIds, colTypes);

        // Verify: if the source person was remapped, the project's lead
        // Ref value should point to the placeholder, not the original ID.
        const peopleRemap = remap.People;
        const projectsRemap = remap.Projects;

        if (peopleRemap?.has(sPersonId)) {
          const personPlaceholder = peopleRemap.get(sPersonId)!;
          const projectPlaceholder = projectsRemap?.get(sProjectId) ?? sProjectId;

          // The lead value for the (possibly remapped) project row
          // should point to the person placeholder.
          const leadDelta = remapped.tableDeltas.Projects.columnDeltas.lead[projectPlaceholder];
          assert.isOk(leadDelta, `Round ${round}: expected lead delta for project`);
          const leadValue = leadDelta[1]![0];  // after-value, unwrapped
          assert.equal(leadValue, personPlaceholder,
            `Round ${round}: lead should point to remapped person placeholder ${personPlaceholder}, got ${leadValue}`);
        }

        const { conflicts } = detectConflicts(left, remapped);
        assert.isTrue(isMergeConflictsEmpty(conflicts),
          `Round ${round}: unexpected conflicts: ${JSON.stringify(conflicts)}`);

        // Simulate apply for People.
        const { newTargetRowIds: newPeople } = simulateApply(
          peopleIds, remapped.tableDeltas.People, peopleRemap,
        );
        // Simulate apply for Projects.
        const { newTargetRowIds: newProjects } = simulateApply(
          projectIds, remapped.tableDeltas.Projects, projectsRemap,
        );

        peopleIds = newPeople;
        projectIds = newProjects;
      }

      // After 3 rounds: 1 original + 3 rounds × 2 = 7 people.
      assert.equal(peopleIds.size, 7);
      // 1 original + 3 rounds × 2 = 7 projects.
      assert.equal(projectIds.size, 7);
    });
  });
});
