// Taxes and public money fixtures (taxes-and-public-money T-6): the shapes
// `/public-money` and `/governor-terms` serve, with invented values for Dane
// County, Wisconsin and the nation.

const DANE = "state:55|county:025";
const WISCONSIN = "state:55";
const NATION = "us:1";

const programs = (shares) =>
  [
    ["social_security", "Social Security"],
    ["medicare", "Medicare"],
    ["medicaid", "Medicaid and other public medical care"],
    ["income_support", "SSI, EITC and SNAP"],
    ["unemployment", "Unemployment insurance"],
    ["other", "Veterans' benefits and everything else"],
  ].map(([program, label], index) => ({ program, label, share_pct: shares[index] }));

const levelsFor = (geoId) =>
  geoId === NATION
    ? [{ geo_id: NATION, role: "nation" }]
    : geoId === WISCONSIN
      ? [{ geo_id: WISCONSIN, role: "state" }, { geo_id: NATION, role: "nation" }]
      : [{ geo_id: geoId, role: "this place" }, { geo_id: WISCONSIN, role: "state" }, { geo_id: NATION, role: "nation" }];

const PAYMENTS = {
  [DANE]: { total_pct: "13.10", programs: programs(["4.60", "3.40", "2.40", "0.90", "0.20", "1.60"]) },
  [WISCONSIN]: { total_pct: "18.40", programs: programs(["6.40", "4.90", "3.40", "1.30", "0.30", "2.10"]) },
  // The nation's unemployment receipts are not published: null, never zero.
  [NATION]: { total_pct: "17.90", programs: programs(["5.70", "4.40", "4.00", "1.80", null, null]) },
};

const INCOME = {
  [DANE]: ["67.00", "20.00", "13.00"],
  [WISCONSIN]: ["63.00", "18.00", "19.00"],
  [NATION]: ["64.00", "18.00", "18.00"],
};

const rateSeries = (start, values) => values.map((value, index) => ({ year: start + index, value }));

export function publicMoneyAnswer(geoId) {
  const levels = levelsFor(geoId);
  const ids = levels.map((level) => level.geo_id);
  return {
    derived: true,
    geo_id: geoId,
    levels,
    payments: ids.map((id) => ({ geo_id: id, year: 2022, ...PAYMENTS[id], unavailable_reason: null })),
    payments_source: "BEA CAINC35 transfer receipts over CAINC1 personal income",
    income_sources: ids.map((id) => ({
      geo_id: id,
      year: 2023,
      parts: [
        { source: "earnings", label: "Earnings from work, and the rest", share_pct: INCOME[id][0] },
        { source: "dividends_interest_rent", label: "Dividends, interest and rent", share_pct: INCOME[id][1] },
        { source: "payments", label: "Payments from government and others", share_pct: INCOME[id][2] },
      ],
    })),
    federal_tax_rate: {
      measure: "federal_tax_rate",
      label: "Federal income tax as a share of adjusted gross income",
      unit: "percent of adjusted gross income",
      source: "IRS Statistics of Income, county and state income data",
      derived_from: "income_tax_share_of_agi",
      caveats: [
        "Income tax after credits over adjusted gross income on residents' returns.",
        "County files carry income tax from tax year 2010 and the nation's series starts in 2020.",
        "The 2018 federal tax law changed what counts as income and tax.",
      ],
      series: [
        ...(ids.includes(DANE) ? [{ geo_id: DANE, points: rateSeries(2010, ["12.8", "12.9", "13.1", "13.0", "13.2", "13.3", "13.1", "13.3", "12.6", "12.8", "13.0", "13.4", "13.5", "13.4"]) }] : []),
        { geo_id: NATION, points: rateSeries(2020, ["11.0", "11.6", "11.5", "11.4"]) },
      ],
    },
    tax_burden: {
      measure: "tax_burden",
      label: "Taxes collected, share of personal income",
      unit: "percent of personal income",
      source: "Census Annual Survey of State and Local Government Finances and Census of Governments; BEA personal income",
      derived_from: "gold_census_govfin.tax_burden",
      caveats: [],
      series: [
        ...(ids.includes(DANE) ? [{ geo_id: DANE, points: [{ year: 2017, value: "4.300" }, { year: 2022, value: "4.100" }] }] : []),
        ...(ids.includes(WISCONSIN) ? [{ geo_id: WISCONSIN, points: rateSeries(2012, ["11.4", "11.3", "11.1", "11.0", "10.9", "10.8", "10.6", "10.5", "10.4", "10.3", "10.2", "10.1", "10.0"]) }] : []),
        { geo_id: NATION, points: rateSeries(2012, ["10.2", "10.3", "10.3", "10.2", "10.2", "10.1", "10.1", "10.0", "10.1", "10.0", "10.2", "10.1", "10.1"]) },
      ],
    },
    property_tax: {
      measure: "property_tax_load",
      label: "Median property tax as a percent of median home value",
      unit: "percent of median home value",
      source: "American Community Survey 5-year estimates, B25103 and B25077",
      period: "2019–2023",
      caveats: ["A ratio of two medians, not any home's own rate."],
      values: ids.map((id) => ({ geo_id: id, value: { [DANE]: "1.62", [WISCONSIN]: "1.58", [NATION]: "0.98" }[id], margin_of_error: "0.03", unavailable_reason: null })),
    },
    caveats: ["Each measure stands on its own; nothing here is a score or a ranking."],
  };
}

const change = (measure, label, start, end, net, reason = null) => ({
  measure,
  label,
  window_start_year: start,
  window_end_year: end,
  place_change: null,
  nation_change: null,
  net_change: net,
  unavailable_reason: reason,
});

const mix = (shares) =>
  ["property", "general_sales", "income", "other"].map((group, index) => ({
    tax_group: group,
    label: { property: "Property", general_sales: "General sales", income: "Individual and corporate income", other: "Other taxes" }[group],
    share_pct: shares[index],
  }));

export function governorTermsAnswer(geoId) {
  return {
    derived: true,
    geo_id: geoId,
    state_code: "WI",
    measures: [
      { measure: "tax_burden", label: "State and local taxes, share of personal income", change_unit: "percentage points", source: "Census Annual Survey of State and Local Government Finances; BEA personal income", basis: "" },
      { measure: "payments_share", label: "Payments from government and others, share of personal income", change_unit: "percentage points", source: "BEA SAINC35 line 1000 over SAINC50 line 10", basis: "" },
    ],
    caveats: ["These are comparisons, not causes: a governor shares credit and blame with the legislature, federal policy, the Federal Reserve and national trends."],
    items: [
      {
        tenure_key: "evers", holder_name: "Tony Evers", party_code: "DEM", party_text: "Democratic Party",
        start_date: "2019-01-07", end_date: null, so_far: true, term_count: 2,
        fiscal: [
          change("tax_burden", "State and local taxes, share of personal income", 2018, 2024, "-0.200"),
          change("payments_share", "Payments from government and others, share of personal income", 2018, 2024, "0.100"),
        ],
        tax_mix_year: 2024, tax_mix: mix(["34.00", "23.00", "33.00", "10.00"]),
      },
      {
        tenure_key: "walker", holder_name: "Scott Walker", party_code: "REP", party_text: "Republican Party",
        start_date: "2011-01-03", end_date: "2019-01-07", so_far: false, term_count: 2,
        fiscal: [
          change("tax_burden", "State and local taxes, share of personal income", 2010, 2018, "-0.900"),
          change("payments_share", "Payments from government and others, share of personal income", 2010, 2018, "-0.600"),
        ],
        tax_mix_year: 2018, tax_mix: mix(["35.00", "22.00", "33.00", "10.00"]),
      },
      {
        tenure_key: "doyle", holder_name: "Jim Doyle", party_code: "DEM", party_text: "Democratic Party",
        start_date: "2003-01-06", end_date: "2011-01-03", so_far: false, term_count: 2,
        fiscal: [
          change("tax_burden", "State and local taxes, share of personal income", 2002, null, null, "start_year_not_published"),
          change("payments_share", "Payments from government and others, share of personal income", 2002, 2010, "0.900"),
        ],
        tax_mix_year: null, tax_mix: [],
      },
    ],
  };
}

export const PUBLIC_MONEY_GEO_IDS = [DANE, WISCONSIN, NATION];
