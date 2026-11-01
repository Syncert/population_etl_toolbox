import { describe, expect, it } from "vitest";

import {
  COVERAGE_DIFFERENCE_POINTS,
  FORBIDDEN_WORDS,
  RECORD_QUESTIONS,
  RECORD_VERBS,
  changeSentence,
  changeVerb,
  chooseSpan,
  compareVerb,
  countyCrimeAnswer,
  forbiddenWordsIn,
  generatedText,
  percentChangeMargin,
  populationAnswer,
  publishedCrimeAnswer,
  recordAnswers,
  recordPeriodLabel,
  rentIncomeAnswer,
  surveyMargin,
  unemploymentAnswer,
} from "../../../apps/web/lib/checkTheRecord";

// Covers: WEB-156 — check-the-record: the four questions are fixed templates
// over published numbers (ADR-0011): only the allowed verbs, none of the
// forbidden words, a survey change named a rise or fall only when its 90
// percent intervals do not overlap, crime with who reported in each month and
// a "may reflect reporting" line when coverage moves, rents and incomes as two
// series compared by their percentages, population change with the program's
// own components, and a city's crime and unemployment sent to its county.

const county = { level: "COUNTY", geoId: "state:55|county:025", name: "Dane County", role: "this place" };
const state = { level: "STATE", geoId: "state:55", name: "Wisconsin", role: "parent" };
const nation = { level: "NATIONAL", geoId: "us:1", name: "United States", role: "parent" };
const city = { level: "PLACE", geoId: "state:55|place:48000", name: "Madison city", role: "this place" };

const measure = (id, metricCode, label = id) => ({
  measure: { id, label, candidates: [metricCode] },
  metric: { metric_code: metricCode, source_code: metricCode.split(":")[0] },
  metricCode,
  publishedAtGrain: true,
});
const RENT = measure("median-gross-rent", "CENSUS_ACS:acs5:B25064_001", "Median gross rent");
const INCOME = measure("median-household-income", "CENSUS_ACS:acs5:B19013_001", "Median household income");
const POP = measure("population-estimate", "CENSUS_PEP:POPESTIMATE", "Resident population estimate");
const UNEMP = measure("unemployment-rate", "BLS:LAU:UNEMP_RATE", "Unemployment rate");
const FBI = measure("violent-crime-rate", "FBI_UCR:summarized_violent_crime:V:offense:rate", "Violent crime rate");
const component = (id, label) => measure(id, `CENSUS_PEP:${id.toUpperCase()}`, label);

const fiveYear = (end, value, moe, extra = {}) => ({ period_start: `${end - 4}-01-01`, period_end: `${end}-12-31`, value: String(value), margin_of_error: moe === null ? null : String(moe), ...extra });
const year = (y, value, extra = {}) => ({ period_start: `${y}-01-01`, period_end: `${y}-12-31`, value: value === null ? null : String(value), ...extra });
const month = (y, m, value, extra = {}) => {
  const mm = String(m).padStart(2, "0");
  return { period_start: `${y}-${mm}-01`, period_end: `${y}-${mm}-28`, value: value === null ? null : String(value), ...extra };
};
const histories = (entries) => new Map(entries.map(([code, geo, rows]) => [`${code}|${geo}`, rows]));

function expectPlainWording(answer) {
  expect(forbiddenWordsIn(generatedText(answer))).toEqual([]);
}

describe("the wording rules", () => {
  it("asks four fixed questions that name no one", () => {
    expect(RECORD_QUESTIONS.map((item) => item.question)).toEqual([
      "Has violent crime gone up here in ten years?",
      "Are rents rising faster than incomes?",
      "Is the population growing or shrinking, and why: births, deaths or migration?",
      "How does unemployment here compare with the state?",
    ]);
  });

  it("allows exactly ADR-0011's verbs", () => {
    expect(RECORD_VERBS).toEqual(["rose", "fell", "was unchanged", "was higher than", "was lower than", "was not distinguishable from"]);
  });

  it("finds degree, cause and judgement words as whole words", () => {
    expect(forbiddenWordsIn("Rent rose sharply because of demand")).toEqual(["sharply", "because"]);
    expect(forbiddenWordsIn("Settled and ledger and bettering")).toEqual([]);
    expect(FORBIDDEN_WORDS).toContain("safer");
  });

  it("calls a survey change a rise or a fall only when the intervals do not overlap", () => {
    expect(changeVerb(1000, 1100, { from: 40, to: 40 })).toBe("rose");
    expect(changeVerb(1000, 1050, { from: 40, to: 40 })).toBe("was not distinguishable from");
    expect(changeVerb(1100, 1000, { from: 10, to: 10 })).toBe("fell");
    // A margin the Bureau coded as "not computed" cannot decide a verb.
    expect(changeVerb(1000, 1100, { from: null, to: 40 })).toBeNull();
    expect(changeVerb(5, 5)).toBe("was unchanged");
    expect(compareVerb(3.1, 2.8)).toBe("was higher than");
    expect(compareVerb(2.8, 3.1)).toBe("was lower than");
    expect(compareVerb(3, 3)).toBe("was not distinguishable from");
    expect(compareVerb(20, 15, { value: 4, other: 3 })).toBe("was not distinguishable from");
  });

  it("reads the Bureau's coded margins: controlled is zero, the others are not numbers", () => {
    expect(surveyMargin({ margin_of_error: "-555555555" })).toBe(0);
    expect(surveyMargin({ margin_of_error: "-333333333" })).toBeNull();
    expect(surveyMargin({ margin_of_error: "40" })).toBe(40);
    expect(surveyMargin({})).toBeNull();
  });

  it("gives a percent change the Bureau's ratio margin", () => {
    // R = 1.2; MOE(R) = sqrt(30² + 1.2²·20²) / 1000 = 0.03841875…
    expect(percentChangeMargin(1000, 20, 1200, 30)).toBeCloseTo(3.8419, 3);
    expect(percentChangeMargin(1000, null, 1200, 30)).toBeNull();
  });

  it("states both values, both periods, the change and its percent", () => {
    expect(changeSentence({ measure: "Median gross rent", place: "Dane County", verb: "rose", from: { value: 900, period: "2010–2014" }, to: { value: 1350, period: "2020–2024" }, kind: "dollars" }))
      .toBe("Median gross rent in Dane County rose from $900 in 2010–2014 to $1,350 in 2020–2024, a change of +$450 (+50.0 percent).");
    expect(changeSentence({ measure: "Median gross rent", place: "Dane County", verb: "was not distinguishable from", from: { value: 900, period: "2015–2019" }, to: { value: 910, period: "2020–2024" }, kind: "dollars" }))
      .toContain("the 2020–2024 value was not distinguishable from the 2015–2019 value");
  });

  it("names a month, a year and a five-year span in words", () => {
    expect(recordPeriodLabel(month(2024, 12, 1))).toBe("December 2024");
    expect(recordPeriodLabel(year(2023, 1))).toBe("2023");
    expect(recordPeriodLabel(fiveYear(2024, 1, 1))).toBe("2020–2024");
  });
});

describe("choosing the two periods", () => {
  it("takes the newest period and the earliest within ten years", () => {
    const rows = [2010, 2012, 2014, 2016, 2024].map((y) => year(y, y));
    const span = chooseSpan(rows);
    expect([span.from.value, span.to.value]).toEqual(["2014", "2024"]);
  });

  it("keeps five-year estimates from sharing a year", () => {
    const rows = [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024].map((end) => fiveYear(end, end, 1));
    const span = chooseSpan(rows, 5);
    // The earliest within ten years that shares no year with 2020–2024.
    expect(recordPeriodLabel(span.from)).toBe("2013–2017");
    expect(chooseSpan(rows.slice(4), 5)).toBeNull();
  });

  it("compares a monthly series with the same month of an earlier year", () => {
    const rows = [month(2013, 1, 22.4), month(2013, 6, 25), month(2014, 1, 20), month(2023, 5, 26), month(2023, 6, 26.8)];
    const span = chooseSpan(rows);
    expect([recordPeriodLabel(span.from), recordPeriodLabel(span.to)]).toEqual(["June 2013", "June 2023"]);
    expect(chooseSpan([month(2013, 1, 22.4), month(2023, 6, 26.8)])).toBeNull();
  });

  it("skips a period published without a number", () => {
    const span = chooseSpan([year(2015, null), year(2016, 10), year(2025, 12)]);
    expect(span.from.period_start).toBe("2016-01-01");
  });
});

describe("has violent crime gone up here", () => {
  const coverage = (share, agencies) => ({ covered_population: String(Math.round(share * 1000)), linked_population: "1000", coverage_share: String(share), coverage_floor: "0.5",
    contributing_agencies: agencies, excluded_multi_county_agencies: [], non_reporting_agencies: [], unknown_population_agencies: [] });
  const rate = (y, m, value, share = 1, agencies = ["A", "B"], reason = null) => ({
    ratio_id: "violent_crime_county_agency_rate", derived: true, geo_id: county.geoId, period_start: `${y}-${String(m).padStart(2, "0")}-01`, period_end: `${y}-${String(m).padStart(2, "0")}-31`,
    value: value === null ? null : String(value), unit: "per 100,000", scale: 100000, unavailable_reason: reason, coverage: coverage(share, agencies),
  });
  const read = (rows) => ({ state: "read", rows, apiPath: "/api/v1/observations/derived?ratio_id=x" });

  it("compares the newest month with the same month ten years before, with who reported in each", () => {
    const answer = countyCrimeAnswer("Dane County", read([rate(2014, 12, 9.0), rate(2020, 6, 8.0), rate(2024, 11, 10.1), rate(2024, 12, 11.24)]), county);
    expect(answer.status).toBe("answered");
    expect(answer.sentences[0]).toBe(
      "The violent crime rate computed from agency reports in Dane County rose from 9 per 100,000 people its reporting agencies serve in December 2014 to 11.2 per 100,000 people its reporting agencies serve in December 2024, a change of +2.2 per 100,000 (+24.9 percent).",
    );
    expect(answer.coverage[0]).toContain("In December 2014, 2 agencies reported");
    expect(answer.coverage[1]).toContain("100 % of the population its agencies serve");
    expect(answer.links[0].href).toContain("/observations/derived");
    expectPlainWording(answer);
  });

  it("says the change may reflect reporting when coverage moves by more than the threshold", () => {
    const answer = countyCrimeAnswer("Dane County", read([rate(2015, 12, 9.0, 0.6, ["A"]), rate(2024, 12, 11.2, 0.98)]), county);
    expect(COVERAGE_DIFFERENCE_POINTS).toBe(10);
    expect(answer.coverage.at(-1)).toContain("may reflect which agencies reported");
    expect(answer.coverage[0]).toContain("1 agency reported");
  });

  it("never reads a month without a rate as zero", () => {
    const answer = countyCrimeAnswer("Rock County", read([rate(2015, 12, null, 0.2, ["A"], "coverage_below_floor"), rate(2024, 12, null, 0.22, ["A"], "coverage_below_floor")]), county);
    expect(answer.status).toBe("unanswered");
    expect(answer.sentences[0]).toContain("too few agencies reported to give a rate");
    expect(generatedText(answer)).not.toMatch(/\b0 per 100,000/);
  });

  it("names the earlier months it could not compare", () => {
    const answer = countyCrimeAnswer("Dane County", read([rate(2014, 12, null, 0.1, ["A"], "coverage_below_floor"), rate(2016, 12, 9), rate(2024, 12, 11)]), county);
    expect(answer.sentences[0]).toContain("December 2016");
    expect(answer.caveats.join(" ")).toContain("December 2014 (too few agencies reported to give a rate)");
  });

  it("states the FBI's coverage for a state", () => {
    const rows = [year(2013, 280, { coverage: { coverage_percent: "95.1" } }), year(2023, 297.4, { coverage: { coverage_percent: "98" } })];
    const answer = publishedCrimeAnswer(state, FBI, rows, "/explore?x");
    expect(answer.sentences[0]).toContain("The FBI's violent crime rate in Wisconsin rose from 280 per 100,000 residents in 2013 to 297.4 per 100,000 residents in 2023");
    expect(answer.coverage[0]).toBe("In 2013, the agencies reporting to the FBI covered 95.1 percent of the population, as the FBI published it.");
    expectPlainWording(answer);
  });
});

describe("are rents rising faster than incomes", () => {
  const rows = (start, step, moe) => [2014, 2019, 2024].map((end, index) => fiveYear(end, start + index * step, moe));

  it("states each series' change and compares the two percentages", () => {
    const answer = rentIncomeAnswer(county, RENT, INCOME, histories([
      [RENT.metricCode, county.geoId, rows(900, 250, 15)],
      [INCOME.metricCode, county.geoId, rows(60000, 5000, 900)],
    ]), []);
    expect(answer.status).toBe("answered");
    expect(answer.sentences[0]).toBe("Median gross rent in Dane County rose from $900 in 2010–2014 to $1,400 in 2020–2024, a change of +$500 (+55.6 percent).");
    expect(answer.sentences[1]).toContain("Median household income in Dane County rose from $60,000 in 2010–2014 to $70,000 in 2020–2024");
    expect(answer.sentences[2]).toBe("From 2010–2014 to 2020–2024, the percent change in median gross rent (+55.6 percent) was higher than the percent change in median household income (+16.7 percent).");
    expect(answer.caveats.join(" ")).toContain("no affordability index");
    expect(answer.charts.map((chart) => chart.id)).toEqual(["rent", "income"]);
    expectPlainWording(answer);
  });

  it("says the percentages are not distinguishable when their margins overlap", () => {
    const answer = rentIncomeAnswer(county, RENT, INCOME, histories([
      [RENT.metricCode, county.geoId, rows(1000, 50, 60)],
      [INCOME.metricCode, county.geoId, rows(60000, 2500, 3000)],
    ]), []);
    expect(answer.sentences[0]).toContain("was not distinguishable from");
    expect(answer.sentences[2]).toContain("was not distinguishable from the percent change in median household income");
  });

  it("does not compare when income was not published for rent's periods", () => {
    const answer = rentIncomeAnswer(city, RENT, INCOME, histories([
      [RENT.metricCode, city.geoId, rows(900, 250, 15)],
      [INCOME.metricCode, city.geoId, rows(60000, 5000, 900).slice(1)],
    ]), []);
    expect(answer.status).toBe("unanswered");
    expect(answer.sentences[0]).toBe("Median household income for Madison city was not published for 2010–2014, the periods rent is compared over, so the two are not compared.");
  });

  it("states values without a verb where a margin is coded", () => {
    const rent = rows(900, 250, 15);
    rent[0] = { ...rent[0], margin_of_error: "-333333333" };
    const answer = rentIncomeAnswer(county, RENT, INCOME, histories([[RENT.metricCode, county.geoId, rent], [INCOME.metricCode, county.geoId, rows(60000, 5000, 900)]]), []);
    expect(answer.sentences[0]).toContain("no usable margin of error");
    expect(answer.sentences[2]).toContain("not compared");
  });
});

describe("is the population growing or shrinking, and why", () => {
  const pop = [year(2015, 500000, { release: "2020" }), year(2020, 540000, { release: "2025" }), year(2025, 575000, { release: "2025" })];
  const components = (values) => new Map([
    ["births", { measure: component("births", "Births"), row: values.births === undefined ? null : year(2025, values.births) }],
    ["deaths", { measure: component("deaths", "Deaths"), row: values.deaths === undefined ? null : year(2025, values.deaths) }],
    ["domestic-migration", { measure: component("domestic-migration", "Net domestic migration"), row: values.domestic === undefined ? null : year(2025, values.domestic) }],
    ["international-migration", { measure: component("international-migration", "Net international migration"), row: values.international === undefined ? null : year(2025, values.international) }],
  ]);

  it("states the change and the program's components, and nothing else", () => {
    const answer = populationAnswer(county, POP, components({ births: 6200, deaths: 4100, domestic: -1200, international: 2300 }), histories([[POP.metricCode, county.geoId, pop]]), { links: [] });
    expect(answer.sentences[0]).toBe("The estimated resident population in Dane County rose from 500,000 in 2015 to 575,000 in 2025, a change of +75,000 (+15.0 percent).");
    expect(answer.sentences[1]).toBe("For 2025, the program estimates 6,200 births, 4,100 deaths, net domestic migration of -1,200 and net international migration of 2,300 in Dane County.");
    expect(answer.caveats.join(" ")).toContain("different vintages of the estimates (2020 and 2025)");
    expectPlainWording(answer);
  });

  it("names a component that was not published", () => {
    const answer = populationAnswer(county, POP, components({ births: 10, deaths: 5 }), histories([[POP.metricCode, county.geoId, pop]]), { links: [] });
    expect(answer.sentences.at(-1)).toBe("Not published for Dane County: net domestic migration and net international migration.");
  });

  it("sends a city to its county for the components", () => {
    const answer = populationAnswer(city, POP, new Map(), histories([[POP.metricCode, city.geoId, pop]]), { componentsElsewhere: "Dane County", links: [] });
    expect(answer.sentences[1]).toContain("not for cities and towns; Dane County's components are on its page");
  });
});

describe("how does unemployment here compare", () => {
  const sa = (status) => ({ dimensions: { seasonal_adjustment_status: status } });
  it("compares the newest month both publish", () => {
    const answer = unemploymentAnswer(county, state, UNEMP, histories([
      [UNEMP.metricCode, county.geoId, [month(2025, 7, 2.6, sa("U")), month(2025, 8, 2.8, sa("U"))]],
      [UNEMP.metricCode, state.geoId, [month(2025, 6, 3.0, sa("U")), month(2025, 7, 3.1, sa("U"))]],
    ]), [], RECORD_QUESTIONS[3].question);
    expect(answer.sentences).toEqual([
      "In July 2025, the unemployment rate in Dane County was 2.6 percent and in Wisconsin 3.1 percent.",
      "Dane County's rate was lower than Wisconsin's.",
    ]);
    expect(answer.charts[0].model.lines.map((line) => line.place.name)).toEqual(["Dane County", "Wisconsin"]);
    expectPlainWording(answer);
  });

  it("does not compare rates on different seasonal-adjustment bases", () => {
    const answer = unemploymentAnswer(county, state, UNEMP, histories([
      [UNEMP.metricCode, county.geoId, [month(2025, 8, 2.8, sa("U"))]],
      [UNEMP.metricCode, state.geoId, [month(2025, 8, 3.1, sa("S"))]],
    ]), [], "q");
    expect(answer.status).toBe("unanswered");
    expect(answer.sentences[0]).toContain("different seasonal-adjustment bases");
  });
});

describe("the section on each page", () => {
  const chapter = (measures) => ({ chapter: { id: "x" }, headline: measures, depth: [], readGrain: "COUNTY", stateContext: false });
  const shown = [chapter([RENT, INCOME, POP, UNEMP, FBI])];
  const base = { shown, histories: new Map(), answers: new Map(), countyCrime: null, countyPage: null, exploreLink: () => "/explore?x" };

  it("sends a city's crime and unemployment questions to its county", () => {
    const answers = recordAnswers({ ...base, level: "PLACE", levels: [city, { ...county, role: "parent" }, state, nation], countyPage: { name: "Dane County", href: "/us/wisconsin/dane-county" } });
    expect(answers.map((answer) => answer.id)).toEqual(["crime", "rent-income", "population", "unemployment"]);
    const crime = answers[0];
    expect(crime.status).toBe("elsewhere");
    expect(crime.links[0]).toEqual({ label: "Dane County's answer", href: "/us/wisconsin/dane-county#record" });
    expect(answers[3].status).toBe("elsewhere");
  });

  it("asks a state about the nation, and the nation about nothing larger", () => {
    const stateAnswers = recordAnswers({ ...base, level: "STATE", levels: [{ ...state, role: "this place" }, nation] });
    expect(stateAnswers[3].question).toBe("How does unemployment here compare with the nation?");
    const nationAnswers = recordAnswers({ ...base, level: "NATIONAL", levels: [{ ...nation, role: "this place" }] });
    expect(nationAnswers[3].sentences[0]).toBe("The nation has no larger area to compare its unemployment rate with.");
  });

  it("waits for a county's crime read without saying anything about crime", () => {
    const answers = recordAnswers({ ...base, level: "COUNTY", levels: [county, state, nation], countyCrime: { state: "reading" } });
    expect(answers[0].status).toBe("reading");
  });
});
