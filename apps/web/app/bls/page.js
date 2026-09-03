import { redirect } from "next/navigation";

// Retired: this route rendered a demonstration dashboard whose trend
// charts, secondary KPIs, ranked lists, related-indicator tables,
// demographic breakdowns, and stylized maps were illustrative examples
// rather than published data. WEB-003 delivered its replacement — the
// capability-driven explorer reaches this source through whichever access
// shape its capability entry declares — so the link keeps working and
// lands on live, source-backed analysis instead.
export default function BlsPage() {
  redirect("/explore?source=bls");
}
