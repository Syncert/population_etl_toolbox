/** Pick a bounded, repeatable part of an active metric catalog. */
export function parseSweepSelection(env) {
  const budget = Number(env.MAP_SWEEP_METRICS ?? 40);
  if (!Number.isInteger(budget) || budget < 1) {
    throw new Error("MAP_SWEEP_METRICS must be a positive integer");
  }
  const all = env.MAP_SWEEP_ALL === "1";
  const hasOffset = env.MAP_SWEEP_OFFSET !== undefined;
  const hasLimit = env.MAP_SWEEP_LIMIT !== undefined;
  if (all && (hasOffset || hasLimit)) {
    throw new Error("MAP_SWEEP_ALL cannot be combined with MAP_SWEEP_OFFSET or MAP_SWEEP_LIMIT");
  }
  if (hasOffset || hasLimit) {
    if (!hasLimit) {
      throw new Error("MAP_SWEEP_LIMIT is required when MAP_SWEEP_OFFSET is set");
    }
    const offset = Number(env.MAP_SWEEP_OFFSET ?? 0);
    const limit = Number(env.MAP_SWEEP_LIMIT);
    if (!Number.isInteger(offset) || offset < 0) {
      throw new Error("MAP_SWEEP_OFFSET must be a nonnegative integer");
    }
    if (!Number.isInteger(limit) || limit < 1) {
      throw new Error("MAP_SWEEP_LIMIT must be a positive integer");
    }
    return { mode: "shard", offset, limit, budget };
  }
  return { mode: all ? "all" : "sample", budget };
}

export function selectSweepMetrics(items, selection) {
  if (selection.mode === "all") {
    return [...items];
  }
  if (selection.mode === "shard") {
    return items.slice(selection.offset, selection.offset + selection.limit);
  }
  if (items.length <= selection.budget) {
    return [...items];
  }
  if (selection.budget === 1) {
    return [items[0]];
  }
  const picked = [];
  for (let index = 0; index < selection.budget; index += 1) {
    picked.push(items[Math.round((index * (items.length - 1)) / (selection.budget - 1))]);
  }
  return [...new Set(picked)];
}

export function makeSweepReport({ selection, sources, results, complete }) {
  const summary = { coloured: 0, narrowed: 0, empty: 0, fail: 0 };
  for (const result of results) {
    summary[result.verdict] += 1;
  }
  return { schema_version: 1, selection, complete, sources, results, summary };
}
