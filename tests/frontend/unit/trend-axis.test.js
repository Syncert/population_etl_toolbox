// Covers: WEB-180 — a trend chart's reference values are round, and a share
// that rounds to nothing says so (website UX review 2026-11, D1 and D4).
import { describe, expect, it } from "vitest";

import { roundTicks } from "../../../apps/web/components/PlaceTrend";
import { shareText } from "../../../apps/web/lib/whereMoneyGoes";

describe("a trend chart's reference values", () => {
  it("are round values inside the drawn range, two to four of them", () => {
    expect(roundTicks(99.66, 200.53)).toEqual([100, 150, 200]);
    expect(roundTicks(1.3, 14)).toEqual([5, 10]);
    expect(roundTicks(331_500_000, 341_784_857)).toEqual([335_000_000, 340_000_000]);
    expect(roundTicks(2.1, 2.9)).toEqual([2.2, 2.4, 2.6, 2.8]);
    for (const [min, max] of [[0, 1], [-40, 65], [88_532, 94_426], [0.031, 0.047], [12, 12.4]]) {
      const ticks = roundTicks(min, max);
      expect(ticks.length).toBeGreaterThanOrEqual(2);
      expect(ticks.length).toBeLessThanOrEqual(4);
      expect(ticks.every((value) => value >= min && value <= max)).toBe(true);
    }
  });

  it("is the one value when nothing varies", () => {
    expect(roundTicks(7, 7)).toEqual([7]);
  });
});

describe("a spending line's share", () => {
  it("says what rounding would hide, and nothing else changes", () => {
    expect(shareText("0.03")).toBe("under 0.1%");
    expect(shareText("0.05")).toBe("0.1%");
    expect(shareText("0")).toBe("0.0%");
    expect(shareText("10")).toBe("10.0%");
    expect(shareText(null)).toBe("not published");
  });
});
