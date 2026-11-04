// Covers: WEB-162 — every map uses one hue light to dark for more and less,
// and a measure running below and above zero meets at a neutral grey.

import { describe, expect, it } from "vitest";

import {
  CHOROPLETH_PALETTE,
  DIVERGING_ABOVE,
  DIVERGING_BELOW,
  DIVERGING_NEUTRAL,
  bandColours,
  colorForValue,
} from "../../../apps/web/lib/explorerViewModel";

const lightness = (hex) => {
  const [r, g, b] = [1, 3, 5].map((at) => parseInt(hex.slice(at, at + 2), 16) / 255);
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
};

describe("map ramp", () => {
  it("runs one ramp from light to dark", () => {
    for (const ramp of [CHOROPLETH_PALETTE, DIVERGING_ABOVE, DIVERGING_BELOW]) {
      const steps = ramp.map(lightness);
      expect(steps).toEqual([...steps].sort((left, right) => right - left));
    }
  });

  it("keeps bands on one side of zero on the one-hue ramp", () => {
    const bands = [[0, 1], [1, 2], [2, 3], [3, 4], [4, 5]].map(([lower, upper]) => ({ lower, upper }));
    expect(bandColours(bands)).toEqual(CHOROPLETH_PALETTE);
    const negative = [[-5, -4], [-4, -3], [-3, -2], [-2, -1], [-1, 0]].map(([lower, upper]) => ({ lower, upper }));
    expect(bandColours(negative)).toEqual(CHOROPLETH_PALETTE);
  });

  it("meets at grey where bands straddle zero, darker away from zero on each arm", () => {
    const bands = [[-9, -6], [-6, -1], [-1, 2], [2, 5], [5, 12]].map(([lower, upper]) => ({ lower, upper }));
    expect(bandColours(bands)).toEqual([DIVERGING_BELOW[1], DIVERGING_BELOW[0], DIVERGING_NEUTRAL, DIVERGING_ABOVE[0], DIVERGING_ABOVE[1]]);
    const edge = [[-4, 0], [0, 3], [3, 6]].map(([lower, upper]) => ({ lower, upper }));
    expect(bandColours(edge)).toEqual([DIVERGING_BELOW[0], DIVERGING_ABOVE[0], DIVERGING_ABOVE[1]]);
  });

  it("paints an equal-width value by the same rule", () => {
    expect(colorForValue(9, -10, 10)).toBe(DIVERGING_ABOVE[1]);
    expect(colorForValue(0, -10, 10)).toBe(DIVERGING_NEUTRAL);
    expect(colorForValue(-9, -10, 10)).toBe(DIVERGING_BELOW[1]);
    expect(colorForValue(9, 0, 10)).toBe(CHOROPLETH_PALETTE[4]);
  });
});
