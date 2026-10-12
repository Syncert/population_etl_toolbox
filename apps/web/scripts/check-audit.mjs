// The production dependency audit, with the way out printed beside it.
//
// The gate is `npm audit --omit=dev --audit-level=high` (WEB-007), unchanged:
// this runs that exact command with its output passed straight through and
// exits with its status, so a failure is never masked and a clean run prints
// nothing more. What it adds is on failure only: the advisories at or above
// the gate's level, where each vulnerable package is pinned (an `overrides`
// entry, a direct dependency, or transitive), the command that moves it, and
// the tiers to run before pushing. `npm audit fix` cannot move a package
// pinned in `overrides` -- it stops with `EOVERRIDE`, which does not say so --
// and that is the case this was written for.
//
// Usage: node scripts/check-audit.mjs   (from apps/web, after `npm ci`)

import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const appRoot = join(here, "..");

export const AUDIT_LEVEL = "high";
export const AUDIT_ARGS = ["audit", "--omit=dev", `--audit-level=${AUDIT_LEVEL}`];

const SEVERITY_RANK = { info: 0, low: 1, moderate: 2, high: 3, critical: 4 };

export const VALIDATION_STEPS = [
  "npm audit --omit=dev --audit-level=high",
  "npm run lint",
  "npm run typecheck",
  "npm run test:unit",
  "npm run build",
  "npm run check:bundle",
  "npm run check:csp",
  "npm run test:browser",
];

function atOrAboveGate(severity) {
  return (SEVERITY_RANK[severity] ?? -1) >= SEVERITY_RANK[AUDIT_LEVEL];
}

function advisoriesOf(vulnerability) {
  return (vulnerability.via ?? []).filter((item) => typeof item === "object");
}

/** Where a vulnerable package's version is decided in package.json. */
export function pinLocation(name, packageJson) {
  if (Object.hasOwn(packageJson.overrides ?? {}, name)) return "overrides";
  if (Object.hasOwn(packageJson.dependencies ?? {}, name)) return "direct dependency";
  return "transitive";
}

function remediation(name, vulnerability, location, packageJson) {
  if (location === "overrides") {
    return [
      `Raise "overrides" -> "${name}" in apps/web/package.json (now ` +
        `${packageJson.overrides[name]}) to a release outside ${vulnerability.range}, ` +
        "then run `npm install` in apps/web so the lockfile follows it.",
      "`npm audit fix` cannot move an override; it stops with EOVERRIDE.",
    ];
  }
  const fix = vulnerability.fixAvailable;
  if (fix && typeof fix === "object") {
    const major = fix.isSemVerMajor ? " (a semver-major change: read its release notes)" : "";
    return [`Run \`npm install ${fix.name}@${fix.version}\` in apps/web${major}.`];
  }
  if (fix === true) {
    return ["Run `npm audit fix` in apps/web (never `--force`), then review the lockfile diff."];
  }
  const parents = (vulnerability.effects ?? []).join(", ") || "its dependents";
  return [
    `No published fix reaches it through ${parents}. Add "${name}" to "overrides" ` +
      "in apps/web/package.json at a release outside " +
      `${vulnerability.range}, then run \`npm install\` in apps/web.`,
  ];
}

/**
 * The remediation report for one `npm audit --json` document, as lines.
 * Empty when nothing reaches the gate's level.
 */
export function explainAudit(report, packageJson) {
  const failing = Object.entries(report.vulnerabilities ?? {})
    .filter(([, vulnerability]) => atOrAboveGate(vulnerability.severity))
    .sort(([left], [right]) => left.localeCompare(right));
  if (failing.length === 0) return [];

  const lines = [
    "",
    `The production audit found ${failing.length} package(s) at or above "${AUDIT_LEVEL}":`,
  ];
  for (const [name, vulnerability] of failing) {
    const location = pinLocation(name, packageJson);
    lines.push("", `${name} (${vulnerability.severity}), pinned as: ${location}`);
    for (const advisory of advisoriesOf(vulnerability)) {
      lines.push(`  - ${advisory.title} [${advisory.range}] ${advisory.url}`);
    }
    for (const via of (vulnerability.via ?? []).filter((item) => typeof item === "string")) {
      lines.push(`  - through ${via}`);
    }
    for (const step of remediation(name, vulnerability, location, packageJson)) {
      lines.push(`  fix: ${step}`);
    }
  }
  lines.push(
    "",
    "Then, from apps/web, run before pushing:",
    ...VALIDATION_STEPS.map((step) => `  ${step}`),
    "The procedure is in apps/web/README.md, \"The dependency audit gate\".",
  );
  return lines;
}

/**
 * Run the gate. `run(args)` returns `{ status, stdout }`; the first call's
 * output is the audit's own and is printed as-is.
 */
export function checkAudit({ run, packageJson, write }) {
  const gate = run(AUDIT_ARGS);
  if (gate.stdout) write(gate.stdout);
  if (gate.status === 0) return 0;

  const detail = run([...AUDIT_ARGS, "--json"]);
  let report;
  try {
    report = JSON.parse(detail.stdout);
  } catch {
    write("\nThe audit's JSON report could not be read; see its output above.\n");
    return gate.status;
  }
  const lines = explainAudit(report, packageJson);
  if (lines.length) write(`${lines.join("\n")}\n`);
  return gate.status;
}

function runNpm(args) {
  const options = { cwd: appRoot, encoding: "utf8", stdio: ["ignore", "pipe", "inherit"] };
  // npm is a .cmd shim on Windows, which only a shell can start. The
  // arguments are this module's constants, so one command line is safe.
  const result =
    process.platform === "win32"
      ? spawnSync(`npm ${args.join(" ")}`, { ...options, shell: true })
      : spawnSync("npm", args, options);
  return { status: result.status ?? 1, stdout: result.stdout ?? "" };
}

if (process.argv[1] && process.argv[1].endsWith("check-audit.mjs")) {
  const packageJson = JSON.parse(readFileSync(join(appRoot, "package.json"), "utf8"));
  process.exitCode = checkAudit({
    run: runNpm,
    packageJson,
    write: (text) => process.stdout.write(text),
  });
}
