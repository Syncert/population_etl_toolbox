import { expect, test } from "vitest";

// Covers: WEB-027 — the tier's process exit code is its tests' verdict.
//
// An unhandled error exits the process non-zero while every test reports
// green, and in CI that is indistinguishable from a red process with red
// tests: the exit code is the only thing the job reads. That is the state
// this tier was built to end, not to reproduce, so the condition gets a test
// to point at. `--dangerouslyIgnoreUnhandledErrors` would do the opposite —
// it keeps the green summary and drops the red exit code, which is worse
// than either.
//
// Why every file calls this rather than one hook covering the tier: an
// unhandled error is only visible in the process that raises it, and each
// file's requests are its own. A guard living in one file leaves every other
// file free to exit the tier non-zero with nothing named — which is exactly
// what `map-wiring.smoke.test.js` did on 2026-09-12, run on its own: two
// tests passed, one unhandled error, exit 1, no failure anywhere in the
// report.
//
// What it catches in practice is an HTTP socket that ends while the response
// parser is still paused (`assert(!this.paused)` inside Node's bundled
// undici). Measured on 2026-09-12, Node 24.20.0, one whole-world tile of
// 864,711 bytes fetched six times:
//
//   origin                          body read   abandoned/cancelled
//   next dev rewrite (connection: close)   6 asserts        0
//   composed nginx proxy (keep-alive)      0                0
//
// So the trigger is the origin closing the socket under a large response,
// not the client's handling of the body: reading it through `arrayBuffer`,
// through `body`'s async iterator, and through a reader all assert equally.
// That is why this tier declares the composed proxy as its origin
// (`tests/run.ps1 web-smoke`) and why an unhandled error here is a finding
// about the deployment shape rather than a flake to retry.
//
// Call it at the end of a test file, so the test it declares runs after the
// ones it reports on.
export function reportUnhandledErrors() {
  // Recorded from module load rather than from a hook, because there is no
  // hook late enough to be the only place they can be seen: a socket tears
  // down after the request that caused it, so an error can land between two
  // tests or after the last one. Node calls every registered listener, so
  // this records alongside Vitest's own reporting rather than swallowing it.
  const unhandled = [];
  process.on("uncaughtException", (error) => unhandled.push(error));
  process.on("unhandledRejection", (reason) => unhandled.push(reason));

  test("the file's requests left no unhandled error behind", async () => {
    // Socket teardown lands after the request that provoked it, so anything
    // already in flight is given a moment to arrive before the verdict.
    await new Promise((resolve) => setTimeout(resolve, 1_000));
    expect(
      unhandled.map((error) => String(error?.message || error).split("\n")[0]),
      "the tests passed but the process did not: an unhandled error here exits non-zero under a green summary",
    ).toEqual([]);
  }, 10_000);
}
