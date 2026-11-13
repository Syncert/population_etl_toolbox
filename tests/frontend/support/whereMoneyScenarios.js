// Where the money goes fixtures (where-public-money-goes slice 5): the shape
// `/where-money-goes` serves. Dane County's fiscal 2024 grants and
// Wisconsin's fiscal 2024 all-award total are USAspending's own figures (the
// checked-in answers); the other amounts are invented.

const DANE = "state:55|county:025";
const WISCONSIN = "state:55";
const NATION = "us:1";

const SEPARATE =
  "These are separate figures from separate sources. Federal awards recorded in a place are not what its own governments spend, and the two are never added.";

const line = (key, label, amount, share, reason = null) => ({ key, label, amount, share_pct: share, unavailable_reason: reason });

function stateLocal(geoId) {
  const county = geoId.includes("|county:");
  return {
    layer: "state_local_spending",
    title: county ? "What local governments in the county area spend" : "What state and local governments spend",
    spender: county ? "local governments in the county area" : "state and local governments",
    source: county ? "Census of Governments, finance Individual Unit Files" : "Census Annual Survey of State and Local Government Finances",
    geo_id: geoId,
    fiscal_year: 2022,
    period_label: "fiscal year ending in 2022",
    unit: "thousands of US dollars",
    total: "3000000",
    total_label: "Direct general spending",
    lines: [
      line("spend_elementary_secondary_education", "Elementary and secondary education", "1405622", "46.85"),
      line("spend_highways", "Highways", "300000", "10.00"),
      line("spend_public_welfare", "Public welfare", "290000", "9.67"),
      line("spend_police", "Police protection", "240000", "8.00"),
      line("spend_health", "Health", "180000", "6.00"),
      line("spend_parks_recreation", "Parks and recreation", "120000", "4.00"),
      line("spend_sewerage", "Sewerage", "110000", "3.67"),
      line("spend_other_unallocable", "Other and unallocable", "354378", "11.81"),
      line("spend_fire", "Fire protection", null, null, "suppressed_input"),
    ],
    notes: [
      county
        ? "Local governments in the county area: the county, its cities, towns, school and special districts, each counted whole in its Census home county. Census counts them only in years ending in 2 and 7."
        : "Direct general spending by the state government and all its local governments, by Census function line; utilities, liquor stores and insurance trust payments (pensions) are outside general spending.",
    ],
    unavailable_reason: null,
  };
}

function awards(geoId, kind) {
  const county = geoId.includes("|county:");
  const byType = kind === "type";
  const base = {
    layer: byType ? "federal_awards_by_type" : "federal_awards_by_agency",
    title: byType ? "Federal awards recorded here, by type" : "Federal awards recorded here, by agency",
    spender: "federal agencies",
    source: "USAspending.gov, spending by geography, place of performance",
    geo_id: geoId,
    fiscal_year: 2024,
    period_label: "fiscal 2024 (October 1, 2023 to September 30, 2024)",
    unit: "US dollars",
    total_label: "All federal award obligations recorded here",
    notes: ["Recorded where the awarded work is performed, which is not always where the money is felt: pass-through grants sit with the state agency that receives them, and some payments (Medicare) where the claims processor is."],
    unavailable_reason: null,
  };
  if (county) {
    // No all-award county total: amounts, no shares, and the reason.
    return {
      ...base,
      total: null,
      lines: byType ? [line("grants", "Grants", "2978864182.74", null, "all-award total not published for this place and year")] : [],
      unavailable_reason: byType ? null : "not published for this place",
    };
  }
  return {
    ...base,
    total: "112441461673.81",
    lines: byType
      ? [
          line("direct_payments", "Direct payments", "56000000000", "49.80"),
          line("grants", "Grants", "38000000000", "33.80"),
          line("contracts", "Contracts", "15000000000", "13.34"),
          line("other_assistance", "Other financial assistance", "2500000000", "2.22"),
          line("loans", "Loans", "-300000000", "-0.27"),
        ]
      : [
          line("HHS", "Department of Health and Human Services", "70000000000", "62.25"),
          line("SSA", "Social Security Administration", "12000000000", "10.67"),
          line("DOD", "Department of Defense", "9000000000", "8.00"),
        ],
  };
}

export function whereMoneyAnswer(geoId) {
  if (geoId === NATION) {
    return {
      geo_id: NATION,
      geo_level: "nation",
      layers: [
        {
          layer: "federal_outlays",
          title: "What the federal government spends",
          spender: "federal government",
          source: "U.S. Treasury, Monthly Treasury Statement, Table 9",
          geo_id: NATION,
          fiscal_year: 2025,
          period_label: "fiscal 2025, statement of 2025-09-30",
          unit: "US dollars",
          total: "7010000000000",
          total_label: "Total outlays",
          lines: [
            line("outlays_social_security", "Social Security", "1580000000000", "22.54"),
            line("outlays_health", "Health", "990000000000", "14.12"),
            line("outlays_net_interest", "Net interest", "1050000000000", "14.98"),
            line("outlays_offsetting", "Undistributed offsetting receipts", "-150000000000", "-2.14"),
          ],
          notes: ["Outlays by budget function from the September statement's fiscal-year-to-date column, as Treasury publishes it."],
          unavailable_reason: null,
        },
        stateLocal(NATION),
      ],
      caveats: [SEPARATE],
    };
  }
  return {
    geo_id: geoId,
    geo_level: geoId.includes("|county:") ? "county" : "state",
    layers: [stateLocal(geoId), awards(geoId, "type"), awards(geoId, "agency")],
    caveats: [SEPARATE],
  };
}

export const WHERE_MONEY_GEO_IDS = [DANE, WISCONSIN, NATION];
