import { describe, expect, test } from "vitest";

// Covers: WEB-021 — the first-wave products are configuration over stable
// catalog identities. Every slot resolves to an identity the catalog
// actually published and displays it, a slot nothing satisfies is reported
// rather than dropped or filled with a similar measure, and partial source
// coverage leaves a stated gap instead of collapsing the product.

import {
  DEFAULT_TEMPLATE_ID,
  PRODUCT_TEMPLATES,
  findTemplate,
  profileExport,
  resolveTemplate,
  templateCoverage,
  templateMetricCodes,
} from "../../../apps/web/lib/productTemplates";
import {
  parseProfileState,
  profileHref,
  serializeProfileState,
} from "../../../apps/web/lib/urlState";

const catalogIndex = (codes) =>
  new Map(
    codes.map((code) => [
      code,
      {
        metric_code: code,
        metric_display_name: `Published name for ${code}`,
        source_code: code.split(":")[0],
        units: "people",
        freshness_state: "fresh",
      },
    ]),
  );

describe("the first-wave products are configuration", () => {
  test("all eight products exist and declare their own limits", () => {
    expect(PRODUCT_TEMPLATES.map((template) => template.id)).toEqual([
      "community-conditions",
      "population-growth",
      "workforce",
      "housing-affordability",
      "aging-population",
      "disease-illness-burden",
      "public-safety-trend",
      "rural-agricultural-economy",
    ]);
    expect(DEFAULT_TEMPLATE_ID).toBe("community-conditions");
    for (const template of PRODUCT_TEMPLATES) {
      // Each product states what it does not claim, so a reader is never
      // left to infer that adjacent measures explain each other.
      expect(template.limits.length).toBeGreaterThan(20);
      expect(template.sections.length).toBeGreaterThan(0);
      for (const section of template.sections) {
        for (const measure of section.measures) {
          // A slot with no candidate identity could only be filled by
          // guessing, which is exactly what these templates must not do.
          expect(measure.candidates.length).toBeGreaterThan(0);
        }
      }
    }
    expect(findTemplate("workforce")?.title).toContain("Workforce");
    expect(findTemplate("not-a-template")).toBeNull();
    expect(findTemplate(null)).toBeNull();
  });

  test("the community profile spans the cross-source conditions it names", () => {
    const community = findTemplate("community-conditions");
    const sources = new Set(
      templateMetricCodes(community).map((code) => code.split(":")[0]),
    );
    // Health, safety, and rural context come from their own sources rather
    // than being inferred from the Census measures beside them.
    expect(sources).toContain("CDC");
    expect(sources).toContain("FBI_UCR");
    expect(sources).toContain("USDA_NASS");
    expect(sources).toContain("CENSUS_PEP");
    expect(sources).toContain("BLS");
    expect(templateMetricCodes(null)).toEqual([]);
  });
});

describe("slots resolve only to identities the catalog published", () => {
  test("the first published candidate fills the slot and is displayed", () => {
    const template = findTemplate("community-conditions");
    // Only the second candidate for total population is published here.
    const resolved = resolveTemplate(
      template,
      catalogIndex(["CENSUS_ACS:acs1:B01003_001", "BLS:LAU:UNEMP_RATE"]),
    );
    const population = resolved[0].measures[0];
    expect(population.available).toBe(true);
    expect(population.metricCode).toBe("CENSUS_ACS:acs1:B01003_001");
    // The resolved identity and the publisher's own name travel with it, so
    // the reader sees which measure answered rather than the slot's label.
    expect(population.metric.metric_display_name).toContain(
      "CENSUS_ACS:acs1:B01003_001",
    );
    expect(population.metric.source_code).toBe("CENSUS_ACS");
  });

  test("a slot nothing satisfies reports what it looked for", () => {
    const template = findTemplate("community-conditions");
    const resolved = resolveTemplate(
      template,
      catalogIndex(["CENSUS_ACS:acs5:B01003_001"]),
    );
    const health = resolved.find((entry) => entry.section.id === "health")
      .measures[0];
    expect(health.available).toBe(false);
    expect(health.metric).toBeNull();
    expect(health.metricCode).toBe("");
    // The reason names the identities, so an operator can see whether the
    // measure is missing or the template is pointing at the wrong code.
    expect(health.reason).toContain("CDC:cdi:ALC1_1:crude");
    expect(health.reason).toContain("no published measure");
  });

  test("an unfilled slot is never filled by a similar measure", () => {
    const template = findTemplate("community-conditions");
    // The catalog publishes an ACS measure but none of the safety
    // candidates. The safety slot must stay empty rather than borrowing it.
    const resolved = resolveTemplate(
      template,
      catalogIndex(["CENSUS_ACS:acs5:B01003_001"]),
    );
    const safety = resolved.find((entry) => entry.section.id === "safety")
      .measures[0];
    expect(safety.available).toBe(false);
    expect(safety.metricCode).not.toBe("CENSUS_ACS:acs5:B01003_001");
  });

  test("partial coverage is counted, not hidden", () => {
    const template = findTemplate("community-conditions");
    const full = resolveTemplate(
      template,
      catalogIndex(templateMetricCodes(template)),
    );
    const fullCoverage = templateCoverage(full);
    expect(fullCoverage.unavailable).toBe(0);
    expect(fullCoverage.available).toBe(fullCoverage.requested);

    const partial = resolveTemplate(
      template,
      catalogIndex(["CENSUS_ACS:acs5:B01003_001"]),
    );
    const partialCoverage = templateCoverage(partial);
    expect(partialCoverage.available).toBe(1);
    expect(partialCoverage.unavailable).toBe(partialCoverage.requested - 1);
    // Every section survives an empty catalog: a profile that dropped its
    // gaps would read as though the place had no such conditions.
    const empty = resolveTemplate(template, new Map());
    expect(empty).toHaveLength(template.sections.length);
    expect(templateCoverage(empty).available).toBe(0);
    expect(templateCoverage(null)).toEqual({
      requested: 0,
      available: 0,
      unavailable: 0,
    });
  });

  test("an absent template resolves to nothing rather than a default product", () => {
    expect(resolveTemplate(null, catalogIndex([]))).toEqual([]);
    expect(
      resolveTemplate(findTemplate("workforce"), null)[0].measures[0].available,
    ).toBe(false);
  });
});

describe("profile URL state", () => {
  test("names the template and the place, and carries no values", () => {
    const state = { template: "workforce", geoId: "state:55|county:025" };
    expect(parseProfileState(`?${serializeProfileState(state)}`)).toEqual(
      state,
    );
    expect(profileHref(state, { template: DEFAULT_TEMPLATE_ID })).toContain(
      "/profiles?template=workforce",
    );
    // Reopening re-asks the catalog and the observations, so a shared link
    // can never present a place as it looked when the link was made.
    expect(serializeProfileState(state)).not.toContain("value");
  });

  test("drops invalid template ids and omits the default", () => {
    expect(parseProfileState("?template=Not%2FValid&place=x")).toEqual({
      geoId: "x",
    });
    expect(parseProfileState("")).toEqual({});
    expect(
      serializeProfileState(
        { template: DEFAULT_TEMPLATE_ID },
        { template: DEFAULT_TEMPLATE_ID },
      ),
    ).toBe("");
    expect(profileHref({}, {})).toBe("/profiles");
  });
});

// Covers: WEB-060 — the profile's file carries every field that qualifies a
// value.
//
// WEB-051 and WEB-053 wrote the rule down for the explorer's export: "a file
// that carried a subset would be this client deciding which part of a
// source's participation basis a reader may have", and the same for every
// field `ObservationUncertainty` publishes. The profile product's export
// carried `margin_of_error` alone, read straight off the row -- so it saw
// only the two fields normalization lifts, and only for a neutral-shaped
// source -- and no coverage column at all.
describe("the profile export carries every published qualifier", () => {
  const sections = [
    {
      section: { id: "health", title: "Health" },
      measures: [
        {
          slot: {
            id: "indicator",
            label: "Indicator",
            metricCodes: ["CDC:cdi:X:crude"],
          },
          metric: {
            metric_code: "CDC:cdi:X:crude",
            metric_display_name: "Indicator",
            source_code: "CDC",
          },
          metricCode: "CDC:cdi:X:crude",
          available: true,
          reason: "",
        },
        {
          slot: {
            id: "yield",
            label: "Yield",
            metricCodes: ["USDA_NASS:corn:YIELD"],
          },
          metric: {
            metric_code: "USDA_NASS:corn:YIELD",
            metric_display_name: "Yield",
            source_code: "USDA_NASS",
          },
          metricCode: "USDA_NASS:corn:YIELD",
          available: true,
          reason: "",
        },
      ],
    },
  ];
  // The neutral shape: every qualifier nested under its own envelope, which
  // is where these fields actually arrive.
  const answers = {
    indicator: {
      state: "ok",
      message: "published",
      row: {
        metric_code: "CDC:cdi:X:crude",
        value: "18.2",
        value_status: "valid",
        unit: "percent",
        period_start: "2022-01-01",
        period_end: "2022-12-31",
        uncertainty: { confidence_lower: "16.9", confidence_upper: "19.5" },
        coverage: {
          population: "5900000",
          coverage_basis: "state resident population",
        },
      },
    },
    yield: {
      state: "ok",
      message: "published",
      row: {
        metric_code: "USDA_NASS:corn:YIELD",
        value: "181.4",
        value_status: "valid",
        unit: "bu / acre",
        period_start: "2024-01-01",
        period_end: "2024-12-31",
        uncertainty: {
          cv_value: "14.7",
          cv_status: "unreliable",
          cv_symbol: "(D)",
        },
      },
    },
  };
  const template = {
    id: "community-conditions",
    title: "Community conditions",
  };
  const place = { geoId: "state:55|county:025", placeName: "Dane County" };

  test("a confidence interval reaches the file", () => {
    const { headings, rows } = profileExport(
      template,
      sections,
      answers,
      place,
    );
    const cell = (row, name) => row[headings.indexOf(name)];
    const indicator = rows.find((row) => row[3] === "CDC:cdi:X:crude");
    expect(cell(indicator, "confidence_lower")).toBe("16.9");
    expect(cell(indicator, "confidence_upper")).toBe("19.5");
    // The margin column is still there and still empty: CDC publishes an
    // interval, not a margin, and an empty cell is not a zero.
    expect(cell(indicator, "margin_of_error")).toBe("");
  });

  test("the coefficient of variation and its unreliability flag reach the file", () => {
    const { headings, rows } = profileExport(
      template,
      sections,
      answers,
      place,
    );
    const cell = (row, name) => row[headings.indexOf(name)];
    const yieldRow = rows.find((row) => row[3] === "USDA_NASS:corn:YIELD");
    expect(cell(yieldRow, "cv_value")).toBe("14.7");
    expect(cell(yieldRow, "cv_status")).toBe("unreliable");
    // The symbol NASS publishes precisely to say an estimate is unreliable.
    expect(cell(yieldRow, "cv_symbol")).toBe("(D)");
  });

  test("every published coverage field travels too", () => {
    const { headings, rows } = profileExport(
      template,
      sections,
      answers,
      place,
    );
    const cell = (row, name) => row[headings.indexOf(name)];
    const indicator = rows.find((row) => row[3] === "CDC:cdi:X:crude");
    expect(cell(indicator, "population")).toBe("5900000");
    expect(cell(indicator, "coverage_basis")).toBe("state resident population");
    // A field this row did not publish is empty, never a zero.
    expect(cell(indicator, "coverage_percent")).toBe("");
  });

  test("an unavailable slot still exports its stated reason", () => {
    const unavailable = [
      {
        section: { id: "safety", title: "Safety" },
        measures: [
          {
            slot: { id: "crime", label: "Crime", metricCodes: ["FBI_UCR:x"] },
            metric: null,
            metricCode: "FBI_UCR:x",
            available: false,
            reason: "no published identity satisfies this slot",
          },
        ],
      },
    ];
    const { headings, rows } = profileExport(template, unavailable, {}, place);
    expect(rows[0][headings.indexOf("availability")]).toBe(
      "no published identity satisfies this slot",
    );
    expect(rows[0][headings.indexOf("value")]).toBe("");
  });
});

describe("the second-wave products are curation, not computation", () => {
  const SECOND_WAVE = [
    "housing-affordability",
    "aging-population",
    "disease-illness-burden",
    "public-safety-trend",
    "rural-agricultural-economy",
  ];

  const secondWave = () =>
    PRODUCT_TEMPLATES.filter((template) => SECOND_WAVE.includes(template.id));

  const everySlot = () =>
    secondWave().flatMap((template) =>
      template.sections.flatMap((section) =>
        section.measures.map((measure) => ({ template, section, measure })),
      ),
    );

  test("each second-wave product exists with sections and a stated limit", () => {
    expect(secondWave().map((template) => template.id)).toEqual(SECOND_WAVE);
    for (const template of secondWave()) {
      expect(template.sections.length).toBeGreaterThan(1);
      // The use-case table names an essential guardrail per product. It
      // travels into `limits` as the thing the product does not claim,
      // because a reader meets the product and never the table.
      expect(template.limits.length).toBeGreaterThan(80);
      expect(template.summary.length).toBeGreaterThan(40);
    }
  });

  test("every slot names explicit candidates and none is a placeholder", () => {
    for (const { template, measure } of everySlot()) {
      expect(measure.candidates.length).toBeGreaterThan(0);
      for (const code of measure.candidates) {
        // `SOURCE:object_key` is what the glossary harvest builds a metric
        // code from, so a candidate that is not that shape cannot resolve
        // against any catalog and would report a gap forever.
        expect(code, `${template.id}/${measure.id}`).toMatch(
          /^[A-Z_]+:[^:]+(:.+)?$/,
        );
        expect(code).not.toMatch(/TODO|FIXME|EXAMPLE|\s/);
      }
    }
  });

  test("no product introduces a score, an index, a ranking, or a rate it computes", () => {
    // The client must not author a composite. The API owns semantics, and a
    // number this layer invented would be a measure nobody published and
    // nobody could trace to a source.
    const forbidden =
      /\b(score|index score|ranking|ranked|composite|weighted|per capita rate|normalized by|divided by|we calculate|we compute)\b/i;
    for (const template of secondWave()) {
      const prose = [
        template.title,
        template.summary,
        template.limits,
        ...template.sections.flatMap((section) => [
          section.title,
          section.description,
          ...section.measures.flatMap((measure) => [
            measure.label,
            measure.note || "",
          ]),
        ]),
      ].join(" ");
      // `limits` is allowed to say a product does *not* do these things, so
      // the check reads the sentences that are not denials.
      const claims = prose
        .split(/(?<=\.)\s+/)
        .filter(
          (sentence) =>
            !/\b(no|not|never|nothing|neither|without)\b/i.test(sentence),
        );
      for (const sentence of claims) {
        expect(sentence, `${template.id}: ${sentence}`).not.toMatch(forbidden);
      }
    }
  });

  test("no copy anywhere claims one measure causes another", () => {
    const causal =
      /\b(causes?|caused by|because of|leads to|drives?|due to|results? in|explains?)\b/i;
    for (const template of secondWave()) {
      for (const section of template.sections) {
        for (const measure of section.measures) {
          const note = measure.note || "";
          // A denial ("no measure explains another") is the opposite of a
          // causal claim and is what these products are supposed to say.
          if (/\b(no|not|never|nothing|neither)\b/i.test(note)) continue;
          expect(note, `${template.id}/${measure.id}`).not.toMatch(causal);
        }
      }
    }
  });

  test("the public-safety product shows count, base, and participation apart", () => {
    // The one second-wave product that reads like an instruction to divide
    // one source by another. It must not: a reported count and a population
    // estimate come from different programs, and the quotient would be a
    // crime rate nobody published.
    const safety = findTemplate("public-safety-trend");
    const sections = safety.sections.map((section) => section.id);
    expect(sections).toContain("reported");
    expect(sections).toContain("participation");
    expect(sections).toContain("population-base");

    const counts = safety.sections.find((section) => section.id === "reported");
    const base = safety.sections.find(
      (section) => section.id === "population-base",
    );
    // Separate sections, so no slot holds both sides of a ratio.
    expect(
      counts.measures.every((measure) =>
        measure.candidates[0].startsWith("FBI_UCR:"),
      ),
    ).toBe(true);
    expect(
      base.measures.some((measure) =>
        measure.candidates[0].startsWith("CENSUS_"),
      ),
    ).toBe(true);
    expect(safety.limits).toMatch(/not zero crime/i);
  });

  test("a missing report is never described as zero", () => {
    // Suppressed, not reported, and zero are three different facts, and the
    // products that carry suppression-bearing sources say so in the slot
    // rather than only in the product's limits.
    const suppressionBearing = [
      "disease-illness-burden",
      "public-safety-trend",
      "rural-agricultural-economy",
    ];
    for (const id of suppressionBearing) {
      const template = findTemplate(id);
      const prose = [
        template.limits,
        ...template.sections.flatMap((section) =>
          section.measures.map((measure) => measure.note || ""),
        ),
      ].join(" ");
      expect(prose, id).toMatch(
        /\b(suppress|not reported|withheld|no agency report)\w*\b/i,
      );
      expect(prose, id).toMatch(/\bzero\b/i);
    }
  });

  test("no second-wave slot names a BLS area series", () => {
    // BLS publishes `source_object_key = series_id`, and a LAU series
    // identifies one area: on the deployed warehouse 12,900 distinct LAU
    // metric codes each serve exactly one geography. A template naming one
    // would answer for a single county and report a gap everywhere else,
    // which is why `BLS:LAU:UNEMP_RATE` in the first wave resolves nowhere.
    // Labor context here comes from ACS tables, which publish one code for
    // every geography.
    for (const { template, measure } of everySlot()) {
      for (const code of measure.candidates) {
        expect(code, `${template.id}/${measure.id}`).not.toMatch(/^BLS:/);
      }
    }
  });

  test("every national series is labelled national where a local one is expected", () => {
    // A FRED series is the United States as a whole. Beside a local ACS
    // measure it reads as local unless it says otherwise, which is the
    // guardrail use case 4 names.
    for (const { template, measure } of everySlot()) {
      if (!measure.candidates.some((code) => code.startsWith("FRED:")))
        continue;
      const prose = `${measure.label} ${measure.note || ""}`;
      expect(prose, `${template.id}/${measure.id}`).toMatch(/national/i);
    }
  });
});
