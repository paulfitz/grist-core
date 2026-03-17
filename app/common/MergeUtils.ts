/**
 * Pure functions for document merge: row ID collision remapping and
 * conflict detection. These operate on ActionSummary objects and have
 * no server dependencies.
 *
 * See MERGE-PLAN.md for the full design.
 */

import {
  ActionSummary, ColumnDelta, TableDelta,
} from "app/common/ActionSummary";
import { extractInfoFromColType, isFullReferencingType, isRefListType } from "app/common/gristTypes";
import { CellDelta } from "app/common/TabularDiff";

import isEqual from "lodash/isEqual";

// --- Types ---

/**
 * Per-table mapping from old rowId to placeholder (negative) rowId.
 */
export interface RowRemap {
  [tableId: string]: Map<number, number>;
}

/**
 * Column type info needed for Ref remapping: which columns are Ref/RefList
 * and which table they reference.
 */
export interface ColumnTypeMap {
  [tableId: string]: {
    [colId: string]: string;  // column type string, e.g. "Ref:People", "RefList:Tags"
  };
}

export interface CellConflict {
  tableId: string;
  colId: string;
  rowId: number;              // post-remap
  ancestor: any;              // CellValue | null
  left: any;                  // CellValue | null
  right: any;                 // CellValue | null
}

export interface RowConflict {
  tableId: string;
  rowId: number;
  type: "delete-update";
  deletedOn: "left" | "right";
}

export interface MergeConflicts {
  cells: CellConflict[];
  rows: RowConflict[];
}

export function isMergeConflictsEmpty(c: MergeConflicts): boolean {
  return c.cells.length === 0 && c.rows.length === 0;
}

export interface CellResolution {
  tableId: string;
  colId: string;
  rowId: number;
  pick: "left" | "right" | "dismiss";
}

export interface RowResolution {
  tableId: string;
  rowId: number;
  pick: "keep" | "delete";
}

export interface MergeResolutions {
  cells: CellResolution[];
  rows: RowResolution[];
  strategy?: "left" | "right";
}

// --- Row ID remapping ---

/**
 * Identify colliding rowIds in rightChanges.addRows and rewrite the
 * summary to use negative placeholder IDs. Also rewrites Ref/RefList
 * cell values inside columnDeltas that point to remapped rows.
 *
 * @param right - The source's ActionSummary (will be deep-cloned, not mutated).
 * @param left - The target's ActionSummary (read-only, used to check addRows).
 * @param targetRowIds - Current rowIds in the target doc, per table.
 * @param columnTypes - Column type info for Ref/RefList detection.
 * @returns The remapped summary and the remap table.
 */
export function remapCollisions(
  right: ActionSummary,
  left: ActionSummary,
  targetRowIds: Map<string, Set<number>>,
  columnTypes: ColumnTypeMap,
): { remapped: ActionSummary; remap: RowRemap } {
  const remap: RowRemap = {};
  let nextPlaceholder = -1;

  // Pass 1: identify collisions and assign placeholders.
  for (const [tableId, td] of Object.entries(right.tableDeltas)) {
    if (tableId.startsWith("_grist_")) { continue; }
    if (td.addRows.length === 0) { continue; }

    const existing = targetRowIds.get(tableId) || new Set<number>();
    const tableRemap = new Map<number, number>();

    for (const rowId of td.addRows) {
      if (existing.has(rowId)) {
        tableRemap.set(rowId, nextPlaceholder);
        nextPlaceholder--;
      }
    }

    if (tableRemap.size > 0) {
      remap[tableId] = tableRemap;
    }
  }

  // If no collisions, return the original summary unchanged.
  if (Object.keys(remap).length === 0) {
    return { remapped: right, remap };
  }

  // Pass 2: rewrite the summary.
  const remapped = rewriteSummary(right, remap, columnTypes);
  return { remapped, remap };
}

/**
 * Rewrite an ActionSummary, replacing rowIds per the remap table and
 * rewriting Ref/RefList cell values that point to remapped rows.
 */
function rewriteSummary(
  summary: ActionSummary,
  remap: RowRemap,
  columnTypes: ColumnTypeMap,
): ActionSummary {
  const result: ActionSummary = {
    tableRenames: summary.tableRenames,
    tableDeltas: {},
  };

  for (const [tableId, td] of Object.entries(summary.tableDeltas)) {
    if (tableId.startsWith("_grist_")) {
      result.tableDeltas[tableId] = td;
      continue;
    }

    const tableRemap = remap[tableId];
    const colTypes = columnTypes[tableId] || {};

    result.tableDeltas[tableId] = rewriteTableDelta(td, tableRemap, colTypes, remap);
  }

  return result;
}

function rewriteTableDelta(
  td: TableDelta,
  tableRemap: Map<number, number> | undefined,
  colTypes: { [colId: string]: string },
  remap: { [refTableId: string]: Map<number, number> },
): TableDelta {
  const remapRowId = (rowId: number) => tableRemap?.get(rowId) ?? rowId;

  const newTd: TableDelta = {
    addRows: td.addRows.map(remapRowId),
    removeRows: td.removeRows.map(remapRowId),
    updateRows: td.updateRows.map(remapRowId),
    columnRenames: td.columnRenames,
    columnDeltas: {},
  };

  for (const [colId, cd] of Object.entries(td.columnDeltas)) {
    const newCd: ColumnDelta = {};

    // Determine if this column is a Ref/RefList and which table it references.
    const colType = colTypes[colId] || "";
    const refInfo = isFullReferencingType(colType) ? extractInfoFromColType(colType) : null;
    const refTableRemap = refInfo && "tableId" in refInfo ? remap[refInfo.tableId] : undefined;
    const isRefList = isRefListType(colType);

    for (const [rowIdStr, cellDelta] of Object.entries(cd)) {
      const oldRowId = Number(rowIdStr);
      const newRowId = remapRowId(oldRowId);

      let newDelta = cellDelta;
      if (refTableRemap) {
        newDelta = rewriteCellDeltaRefs(cellDelta, refTableRemap, isRefList);
      }

      newCd[newRowId] = newDelta;
    }

    newTd.columnDeltas[colId] = newCd;
  }

  return newTd;
}

/**
 * Rewrite Ref/RefList values inside a CellDelta.
 *
 * CellDelta is [before, after] where each is [value] | "?" | null.
 * For Ref: value is a plain integer (the rowId).
 * For RefList: value is ["L", id1, id2, ...].
 */
function rewriteCellDeltaRefs(
  delta: CellDelta,
  refTableRemap: Map<number, number>,
  isRefList: boolean,
): CellDelta {
  return [
    rewriteCellDeltaHalf(delta[0], refTableRemap, isRefList),
    rewriteCellDeltaHalf(delta[1], refTableRemap, isRefList),
  ];
}

function rewriteCellDeltaHalf(
  half: [any] | "?" | null,
  refTableRemap: Map<number, number>,
  isRefList: boolean,
): [any] | "?" | null {
  if (half === null || half === "?") { return half; }
  const value = half[0];
  if (isRefList) {
    return [rewriteRefListValue(value, refTableRemap)];
  } else {
    return [rewriteRefValue(value, refTableRemap)];
  }
}

export function rewriteRefValue(value: any, remap: Map<number, number>): any {
  if (typeof value !== "number" || value === 0) { return value; }
  return remap.get(value) ?? value;
}

export function rewriteRefListValue(value: any, remap: Map<number, number>): any {
  if (!Array.isArray(value) || value[0] !== "L") { return value; }
  return ["L", ...value.slice(1).map((id: any) =>
    typeof id === "number" ? (remap.get(id) ?? id) : id,
  )];
}

// --- Conflict detection ---

/**
 * Result of conflict detection, including a set of convergent cell keys
 * that can be used to strip no-op edits from the resolved summary.
 */
export interface DetectConflictsResult {
  conflicts: MergeConflicts;
  /** Keys like "tableId:colId:rowId" for cells where both sides set the same value. */
  convergentKeys: Set<string>;
}

/**
 * Detect conflicts between two ActionSummaries. The right summary
 * should already be remapped (Step 3 done).
 *
 * Also identifies convergent edits (both sides set the same value)
 * and returns their keys so the caller can strip them from the
 * resolved summary without a second pass.
 */
export function detectConflicts(
  left: ActionSummary,
  right: ActionSummary,
): DetectConflictsResult {
  const cells: CellConflict[] = [];
  const rows: RowConflict[] = [];
  const convergentKeys = new Set<string>();

  for (const [tableId, rightTd] of Object.entries(right.tableDeltas)) {
    if (tableId.startsWith("_grist_")) { continue; }
    const leftTd = left.tableDeltas[tableId];
    if (!leftTd) { continue; }

    // Cell conflicts: both sides updated the same (colId, rowId).
    const leftUpdateSet = new Set(leftTd.updateRows);
    const rightUpdateSet = new Set(rightTd.updateRows);
    const bothUpdated = [...rightUpdateSet].filter(r => leftUpdateSet.has(r));

    for (const rowId of bothUpdated) {
      for (const [colId, rightCd] of Object.entries(rightTd.columnDeltas)) {
        const rightDelta = rightCd[rowId];
        if (!rightDelta) { continue; }

        const leftCd = leftTd.columnDeltas[colId];
        const leftDelta = leftCd?.[rowId];
        if (!leftDelta) { continue; }

        // Both sides changed this cell. Check for convergence.
        const leftAfter = leftDelta[1];
        const rightAfter = rightDelta[1];
        if (isEqual(leftAfter, rightAfter)) {
          // Convergent edit — same value. Not a conflict.
          // Record the key so the caller can strip it from the summary.
          convergentKeys.add(`${tableId}:${colId}:${rowId}`);
          continue;
        }

        cells.push({
          tableId,
          colId,
          rowId,
          ancestor: extractValue(leftDelta[0]),
          left: extractValue(leftAfter),
          right: extractValue(rightAfter),
        });
      }
    }

    // Row conflicts: deleted on one side, updated on the other.
    // Left deleted, right updated or added.
    for (const rowId of leftTd.removeRows) {
      if (rightUpdateSet.has(rowId)) {
        rows.push({ tableId, rowId, type: "delete-update", deletedOn: "left" });
      }
    }

    // Right deleted, left updated.
    for (const rowId of rightTd.removeRows) {
      if (leftUpdateSet.has(rowId)) {
        rows.push({ tableId, rowId, type: "delete-update", deletedOn: "right" });
      }
    }
  }

  return { conflicts: { cells, rows }, convergentKeys };
}

/**
 * Remove convergent edits from a summary using pre-computed keys
 * from detectConflicts. More efficient than re-scanning all cells.
 */
export function removeConvergentEdits(
  right: ActionSummary,
  convergentKeys: Set<string>,
): ActionSummary {
  if (convergentKeys.size === 0) { return right; }

  const result: ActionSummary = {
    tableRenames: right.tableRenames,
    tableDeltas: {},
  };
  for (const [tableId, rtd] of Object.entries(right.tableDeltas)) {
    const newTd: TableDelta = {
      addRows: rtd.addRows,
      removeRows: rtd.removeRows,
      updateRows: [...rtd.updateRows],
      columnRenames: rtd.columnRenames,
      columnDeltas: {},
    };
    for (const [colId, rcd] of Object.entries(rtd.columnDeltas)) {
      const newCd: ColumnDelta = {};
      for (const [rowIdStr, rDelta] of Object.entries(rcd)) {
        if (convergentKeys.has(`${tableId}:${colId}:${rowIdStr}`)) {
          continue;
        }
        newCd[Number(rowIdStr)] = rDelta;
      }
      if (Object.keys(newCd).length > 0) {
        newTd.columnDeltas[colId] = newCd;
      }
    }
    const rowsWithDeltas = new Set<number>();
    for (const cd of Object.values(newTd.columnDeltas)) {
      for (const rowIdStr of Object.keys(cd)) {
        rowsWithDeltas.add(Number(rowIdStr));
      }
    }
    const addRowsSet = new Set(newTd.addRows);
    newTd.updateRows = newTd.updateRows.filter(r => rowsWithDeltas.has(r) || addRowsSet.has(r));
    result.tableDeltas[tableId] = newTd;
  }
  return result;
}

/**
 * Extract the actual value from a CellDelta half.
 * [value] → value, "?" → "?", null → null.
 */
function extractValue(half: [any] | "?" | null): any {
  if (half === null || half === "?") { return half; }
  return half[0];
}

// --- Conflict resolution ---

/**
 * Apply resolutions to the right (source) summary, removing or
 * keeping conflicting changes as specified.
 *
 * Returns a new summary with resolved changes. The original is not mutated.
 */
export function applyResolutions(
  right: ActionSummary,
  conflicts: MergeConflicts,
  resolutions: MergeResolutions,
): ActionSummary {
  // Build lookup for cell resolutions.
  const cellPicks = new Map<string, "left" | "right" | "dismiss">();
  for (const res of resolutions.cells) {
    cellPicks.set(`${res.tableId}:${res.colId}:${res.rowId}`, res.pick);
  }

  // Build lookup for row resolutions.
  const rowPicks = new Map<string, "keep" | "delete">();
  for (const res of resolutions.rows) {
    rowPicks.set(`${res.tableId}:${res.rowId}`, res.pick);
  }

  const result: ActionSummary = {
    tableRenames: right.tableRenames,
    tableDeltas: {},
  };

  for (const [tableId, td] of Object.entries(right.tableDeltas)) {
    result.tableDeltas[tableId] = resolveTableDelta(
      tableId, td, conflicts, cellPicks, rowPicks, resolutions.strategy,
    );
  }

  return result;
}

function resolveTableDelta(
  tableId: string,
  td: TableDelta,
  conflicts: MergeConflicts,
  cellPicks: Map<string, "left" | "right" | "dismiss">,
  rowPicks: Map<string, "keep" | "delete">,
  strategy?: "left" | "right",
): TableDelta {
  // Start with a copy.
  const newTd: TableDelta = {
    addRows: [...td.addRows],
    removeRows: [...td.removeRows],
    updateRows: [...td.updateRows],
    columnRenames: td.columnRenames,
    columnDeltas: {},
  };

  // Copy column deltas, dropping cells where resolution is 'left' or 'dismiss'.
  for (const [colId, cd] of Object.entries(td.columnDeltas)) {
    const newCd: ColumnDelta = {};
    for (const [rowIdStr, cellDelta] of Object.entries(cd)) {
      const rowId = Number(rowIdStr);
      const key = `${tableId}:${colId}:${rowId}`;
      const pick = cellPicks.get(key) ?? strategy;
      if (pick === "left" || pick === "dismiss") {
        // Drop this cell from the source changes — target's value wins.
        continue;
      }
      newCd[rowId] = cellDelta;
    }
    if (Object.keys(newCd).length > 0) {
      newTd.columnDeltas[colId] = newCd;
    }
  }

  // Handle row conflicts resolved as 'delete'.
  const rowConflictsForTable = conflicts.rows.filter(r => r.tableId === tableId);
  for (const rc of rowConflictsForTable) {
    const pick = rowPicks.get(`${tableId}:${rc.rowId}`) ?? (strategy === "left" ? "delete" : "keep");
    if (rc.deletedOn === "left" && pick === "delete") {
      // Left deleted, user confirms deletion. Remove source's updates for this row.
      newTd.updateRows = newTd.updateRows.filter(r => r !== rc.rowId);
      for (const cd of Object.values(newTd.columnDeltas)) {
        delete cd[rc.rowId];
      }
    }
    if (rc.deletedOn === "right" && pick === "keep") {
      // Right deleted, user wants to keep. Remove the deletion from source changes.
      newTd.removeRows = newTd.removeRows.filter(r => r !== rc.rowId);
      for (const cd of Object.values(newTd.columnDeltas)) {
        delete cd[rc.rowId];
      }
    }
  }

  // Clean up empty column deltas.
  for (const [colId, cd] of Object.entries(newTd.columnDeltas)) {
    if (Object.keys(cd).length === 0) {
      delete newTd.columnDeltas[colId];
    }
  }

  return newTd;
}
