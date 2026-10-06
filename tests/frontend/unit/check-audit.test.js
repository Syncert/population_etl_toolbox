import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

import {
  AUDIT_ARGS,
  VALIDATION_STEPS,
  checkAudit,
  explainAudit,
  pinLocation,
} from "../../../apps/web/scripts/check-audit.mjs";

// WEB-007: the production audit gate keeps its command and its status, and on
// failure says where each vulnerable package is pinned and how to move it.

const here = path.dirname(fileURLToPath(import.meta.url));
const fixture = (name) =>
  readFileSync(path.join(here, "../support/npm-audit", name), "utf8");

const PACKAGE_JSON = {
  dependencies: { next: "16.0.0", react: "19.0.0" },
  overrides: { nanoid: "3.3.18", sharp: "0.35.4" },
};

function gate({ status, report }) {
  const calls = [];
  const written = [];
  const exitCode = checkAudit({
    packageJson: PACKAGE_JSON,
    write: (text) => written.push(text),
    run: (args) => {
      calls.push(args);
      return args.includes("--json")
        ? { status, stdout: fixture(report) }
        : { status, stdout: "the audit's own output\n" };
    },
  });
  return { calls, exitCode, output: written.join("") };
}

describe("check-audit", () => {
  it("runs the gate's exact command first and keeps its own output", () => {
    const { calls, output } = gate({ status: 0, report: "clean.json" });
    expect(calls[0]).toEqual(["audit", "--omit=dev", "--audit-level=high"]);
    expect(AUDIT_ARGS).toEqual(calls[0]);
    expect(output).toBe("the audit's own output\n");
  });

  it("exits zero and adds nothing when the audit passes", () => {
    const { calls, exitCode, output } = gate({ status: 0, report: "clean.json" });
    expect(exitCode).toBe(0);
    expect(calls).toHaveLength(1);
    expect(output).toBe("the audit's own output\n");
  });

  it("explains an override-pinned advisory and exits with the audit's status", () => {
    const { exitCode, output } = gate({ status: 1, report: "override-pinned.json" });
    expect(exitCode).toBe(1);
    expect(output).toContain("sharp (high), pinned as: overrides");
    expect(output).toContain("sharp heap buffer overflow when decoding a crafted image");
    expect(output).toContain('Raise "overrides" -> "sharp" in apps/web/package.json (now 0.35.4)');
    expect(output).toContain("EOVERRIDE");
    for (const step of VALIDATION_STEPS) expect(output).toContain(step);
  });

  it("names a transitive package and the command that moves it", () => {
    const lines = explainAudit(JSON.parse(fixture("override-pinned.json")), PACKAGE_JSON);
    const text = lines.join("\n");
    expect(text).toContain("source-map-js (high), pinned as: transitive");
    expect(text).toContain("Run `npm audit fix` in apps/web (never `--force`)");
    expect(text).toContain("next (high), pinned as: direct dependency");
    expect(text).toContain("  - through sharp");
  });

  it("leaves advisories below the gate's level out of the report", () => {
    const text = explainAudit(JSON.parse(fixture("override-pinned.json")), PACKAGE_JSON).join("\n");
    expect(text).not.toContain("nanoid");
    expect(text).toContain("found 3 package(s)");
  });

  it("reports nothing for a clean audit document", () => {
    expect(explainAudit(JSON.parse(fixture("clean.json")), PACKAGE_JSON)).toEqual([]);
  });

  it("never masks a failure whose JSON report cannot be read", () => {
    const written = [];
    const exitCode = checkAudit({
      packageJson: PACKAGE_JSON,
      write: (text) => written.push(text),
      run: (args) => ({ status: 1, stdout: args.includes("--json") ? "not json" : "" }),
    });
    expect(exitCode).toBe(1);
    expect(written.join("")).toContain("could not be read");
  });

  it("locates a pin in overrides before the dependency list", () => {
    expect(pinLocation("sharp", { ...PACKAGE_JSON, dependencies: { sharp: "0.35.4" } })).toBe(
      "overrides",
    );
    expect(pinLocation("react", PACKAGE_JSON)).toBe("direct dependency");
    expect(pinLocation("source-map-js", PACKAGE_JSON)).toBe("transitive");
  });
});
