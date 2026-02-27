import type { EChartsOption } from "echarts";

// Evidence.dev's default 10-color palette
const EVIDENCE_PALETTE = [
  "#236aa4",
  "#45a1bf",
  "#a5cdee",
  "#8dacbf",
  "#85c7c6",
  "#d2c6ac",
  "#f4b548",
  "#8f3d56",
  "#71b9f4",
  "#46a485",
];

function getCssVar(name: string): string {
  return getComputedStyle(document.documentElement)
    .getPropertyValue(name)
    .trim();
}

export function getEchartsTheme(): EChartsOption {
  const bg = getCssVar("--background");
  const fg = getCssVar("--foreground");
  const muted = getCssVar("--muted-foreground");
  const border = getCssVar("--border");

  return {
    color: EVIDENCE_PALETTE,
    backgroundColor: "transparent",
    textStyle: { fontFamily: "Inter, sans-serif" },
    title: {
      padding: 0,
      itemGap: 7,
      top: "1px",
      textStyle: { fontSize: 14, color: fg },
      subtextStyle: { fontSize: 13, color: muted, overflow: "break" },
    },
    line: {
      itemStyle: { borderWidth: 2 },
      lineStyle: { width: 2, join: "round" },
      symbolSize: 0,
      symbol: "circle",
      smooth: false,
    },
    categoryAxis: {
      axisLine: { show: true, lineStyle: { color: muted } },
      axisTick: { show: false },
      axisLabel: { color: muted },
      splitLine: { show: false },
    },
    valueAxis: {
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: muted },
      splitLine: { show: true, lineStyle: { color: border, width: 1 } },
    },
    grid: {
      left: "0.8%",
      right: "3%",
      bottom: "0%",
      top: "15%",
      containLabel: true,
    },
    tooltip: {
      backgroundColor: bg,
      borderColor: border,
      borderWidth: 1,
      borderRadius: 4,
      textStyle: { fontSize: 12, fontWeight: 400, color: fg },
      padding: 6,
      extraCssText:
        "box-shadow: 0 3px 6px rgba(0,0,0,.15), 0 2px 4px rgba(0,0,0,.12);",
    },
    legend: {
      icon: "circle",
      pageIconSize: 12,
      animationDurationUpdate: 300,
      textStyle: {
        color: muted,
        padding: [0, 0, 0, -7],
      },
    },
  };
}
