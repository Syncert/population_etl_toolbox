// The metrics the Starting a family rows read directly
// (starting-a-family-collection), in their own module so the place page's
// one catalog read can name them without carrying the rows' code: the rows
// themselves are `lib/familyCollection.ts`, loaded with their lazy chapter.
export const FAMILY_CODES: readonly string[] = [
  "DOL_CHILDCARE:center_infant",
  "HUD_FMR_IL:fmr_3br",
  "CENSUS_ACS:acs5:B25077_001",
  "BLS_QCEW:annual_avg_weekly_wage:10:0",
  "CENSUS_SAIPE_SAHIE:saipe:SAEPOVRT0_17",
];
