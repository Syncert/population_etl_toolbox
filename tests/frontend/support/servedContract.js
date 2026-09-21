// The reviewed API contract, read rather than copied.
//
// The frontend suites stand a fake `/catalog/capabilities` in front of the
// client, and each fake declares the query parameters its routes accept.
// The client is built to send only declared parameters, so those lists
// decide what every frontend test can observe -- and three of them carried a
// comment claiming to be the served list while missing a parameter the API
// had gained. A fixture that models a weaker API than the one that ships
// does not fail; it quietly stops testing the behaviour it names (WEB-043).
//
// `tests/fixtures/api/openapi_contract.json` is the reviewed snapshot the
// API's own suite pins, regenerated deliberately with
// `python -m tests.support.regenerate_openapi_contract`. Reading it here
// means one place changes when the contract does.

import { existsSync, readFileSync } from "node:fs";
import { dirname, join, parse } from "node:path";

/**
 * The snapshot, found by walking up from the working directory.
 *
 * Not `import.meta.url`: Playwright transpiles a `.js` spec and its imports
 * as CommonJS, where `import.meta` is a syntax error, and both the vitest and
 * Playwright runs start from `apps/web`. Walking up for the file itself works
 * from either, and from the repository root.
 */
function findSnapshot() {
  let directory = process.cwd();
  for (;;) {
    const candidate = join(directory, "tests", "fixtures", "api", "openapi_contract.json");
    if (existsSync(candidate)) {
      return candidate;
    }
    const parent = dirname(directory);
    if (parent === directory || directory === parse(directory).root) {
      throw new Error("the reviewed OpenAPI snapshot was not found above the working directory");
    }
    directory = parent;
  }
}

export const SNAPSHOT_PATH = findSnapshot();

const snapshot = JSON.parse(readFileSync(SNAPSHOT_PATH, "utf8"));

/** Query parameter names per `GET` path, exactly as the snapshot declares them. */
const QUERY_PARAMETERS = new Map(
  Object.entries(snapshot.operations || {})
    .filter(([operation]) => operation.startsWith("GET "))
    .map(([operation, body]) => [
      operation.slice("GET ".length),
      (body.parameters || [])
        .filter((parameter) => parameter.in === "query")
        .map((parameter) => parameter.name)
        .sort(),
    ]),
);

/** True when the snapshot declares a `GET` operation at `path`. */
export function servesPath(path) {
  return QUERY_PARAMETERS.has(path);
}

/** Every path the snapshot declares an operation at, whatever the method. */
const ADDRESSABLE_PATHS = new Set(
  Object.keys(snapshot.operations || {}).map((operation) =>
    operation.slice(operation.indexOf(" ") + 1),
  ),
);

/**
 * True when the snapshot declares *any* operation at `path`.
 *
 * Distinct from `servesPath`, which answers only for `GET`, and the
 * distinction is not pedantry: the query-parameter checks below are
 * `GET`-shaped by nature -- a `POST` carries a body, not a query -- so
 * widening that map would make it answer an empty parameter list for routes
 * that have no query at all, which reads as "this route takes no parameters"
 * rather than "this question does not apply".
 *
 * What needs the wider question is the transport-boundary sweep. It reads the
 * client's resource literals and cannot see which method sits beside each one
 * in an options object, so the honest granularity for it is the path. Before
 * ADR-0005 every route this client addressed was a `GET` or a write beside a
 * `GET` on the same path, so `servesPath` happened to answer correctly; the
 * sign-in routes are the first that are `POST`-only.
 */
export function addressesServedPath(path) {
  return ADDRESSABLE_PATHS.has(path);
}

/**
 * The query parameters the API serves at `path`.
 *
 * Throws for a path the snapshot does not declare: a fixture naming a route
 * that is not served is describing an API nobody runs, and returning an
 * empty list would let it pass.
 */
export function servedParameters(path) {
  const parameters = QUERY_PARAMETERS.get(path);
  if (parameters === undefined) {
    throw new Error(`the reviewed contract serves no GET ${path}`);
  }
  return [...parameters];
}

/**
 * Every field name the contract publishes, from every schema.
 *
 * The union rather than one schema's: a claim naming `value_status` or
 * `coverage` is naming a field of whichever envelope publishes it, and a
 * reader following the name does not care which schema it came from.
 */
export function servedFieldNames() {
  const names = new Set();
  for (const schema of Object.values(snapshot.schemas || {})) {
    for (const field of Object.keys(schema.properties || {})) {
      names.add(field);
    }
  }
  return names;
}

/**
 * Every `snake_case` word the contract mentions anywhere.
 *
 * Wider than the field names on purpose: `scope=as_released` names a
 * *value* a parameter accepts, and a client's prose points a reader at it
 * the same way it points at a field. A name absent from the whole document
 * is one the API does not use at all.
 */
export function servedContractWords() {
  const found = JSON.stringify(snapshot).match(/[a-z][a-z0-9]*(?:_[a-z0-9]+)+/g);
  return new Set(found || []);
}

/**
 * The served `GET` paths a fixture's path expression names.
 *
 * Several fixtures build their per-source routes from a template --
 * `` `/api/v1/${segment}/observations/latest` `` -- so the literal text is
 * not a path. Each `${...}` stands for one path segment, and the answer is
 * every served path of that shape: the four source segments, which declare
 * the same parameters as each other.
 */
export function servedPathsMatching(pathExpression) {
  if (!pathExpression.includes("${")) {
    return servesPath(pathExpression) ? [pathExpression] : [];
  }
  const shape = new RegExp(
    `^${pathExpression
      .split(/\$\{[^}]*\}/)
      .map((literal) => literal.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
      .join("[^/]+")}$`,
  );
  return [...QUERY_PARAMETERS.keys()].filter((path) => shape.test(path)).sort();
}

/**
 * The served parameters for `path` without `names`.
 *
 * For a fixture that deliberately models a source declaring fewer filters
 * than the route accepts -- which is how the suite proves the client never
 * sends an undeclared parameter. Removing a name the route does not serve is
 * a typo rather than a narrowing, so it throws.
 */
export function servedParametersWithout(path, names) {
  const parameters = servedParameters(path);
  const unknown = names.filter((name) => !parameters.includes(name));
  if (unknown.length > 0) {
    throw new Error(
      `GET ${path} does not serve ${unknown.join(", ")}, so it cannot be narrowed away`,
    );
  }
  return parameters.filter((name) => !names.includes(name));
}

/**
 * The served `GET` operation a concrete request path belongs to, or `null`.
 *
 * A literal match wins outright; otherwise the templated paths are tried,
 * with each `{name}` standing for exactly one segment. Returning `null` for
 * an unserved path is deliberate -- the caller decides whether that is a
 * failure, and for a request the application actually made it is.
 */
export function servedOperationFor(path) {
  if (QUERY_PARAMETERS.has(path)) {
    return path;
  }
  for (const candidate of QUERY_PARAMETERS.keys()) {
    if (!candidate.includes("{")) {
      continue;
    }
    const shape = new RegExp(
      `^${candidate
        .split(/\{[^}]*\}/)
        .map((literal) => literal.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
        .join("[^/]+")}$`,
    );
    if (shape.test(path)) {
      return candidate;
    }
  }
  return null;
}

const API_PREFIX = "/api/v1";

/**
 * The complaint one request earns, or `null` when it is well formed.
 *
 * Applied by the browser tier to every request its pages make (WEB-052).
 * It lives here, beside the snapshot it reads, so the unit tier can grade it
 * without importing Playwright.
 */
export function requestComplaint(rawUrl) {
  const url = new URL(rawUrl);
  if (!url.pathname.startsWith(API_PREFIX)) {
    return null;
  }
  const operation = servedOperationFor(url.pathname);
  if (operation === null) {
    // `servedOperationFor` resolves against the `GET` operations, because the
    // question below -- "did the client send a query parameter this route does
    // not declare" -- is a `GET` question: a write carries a body, and there is
    // no parameter list to grade it against. A path the contract serves only
    // under another method is therefore not undeclared; it is out of scope for
    // this check, and saying so is different from saying it does not exist.
    // The sign-in routes are the first `POST`-only ones this client calls.
    if (addressesServedPath(url.pathname)) {
      return null;
    }
    return `${url.pathname} is not a path the reviewed contract serves`;
  }
  const declared = new Set(servedParameters(operation));
  const undeclared = [...new Set(url.searchParams.keys())]
    .filter((name) => !declared.has(name))
    .sort();
  if (undeclared.length === 0) {
    return null;
  }
  return (
    `${url.pathname} was sent ${undeclared.join(", ")}, which GET ${operation} ` +
    `does not declare; it accepts ${[...declared].sort().join(", ") || "no parameters"}`
  );
}


/**
 * The property names one reviewed response schema declares.
 *
 * Throws for a schema the snapshot does not carry: a test naming a shape the
 * API does not publish is describing a contract nobody serves, and an empty
 * list would let it pass.
 */
export function servedSchemaFields(name) {
  const schema = (snapshot.schemas || {})[name];
  if (schema === undefined) {
    throw new Error(`the reviewed contract declares no schema ${name}`);
  }
  return Object.keys(schema.properties || {}).sort();
}
