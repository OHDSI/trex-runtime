import ReactEChartsCore from "echarts-for-react/lib/core";
import * as echarts from "echarts/core";
import { BarChart as EBarChart } from "echarts/charts";
import {
  GridComponent,
  TooltipComponent,
  LegendComponent,
  TitleComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import { getEchartsTheme } from "../echarts-theme";
import { useQueryData } from "./QueryProvider";

echarts.use([
  EBarChart,
  GridComponent,
  TooltipComponent,
  LegendComponent,
  TitleComponent,
  CanvasRenderer,
]);

interface BarChartProps {
  x: string;
  y: string;
  title?: string;
  horizontal?: boolean;
  data?: string;
  query?: string;
}

export function BarChart({ x, y, title, horizontal, data, query }: BarChartProps) {
  const queryResult = useQueryData(query ?? "");
  const rows = data ? JSON.parse(data) : queryResult.data;
  const theme = getEchartsTheme();

  const xValues = rows.map((r: Record<string, unknown>) => r[x]);
  const yValues = rows.map((r: Record<string, unknown>) => r[y]);

  const option = {
    ...theme,
    title: title ? { ...theme.title, text: title } : undefined,
    tooltip: { ...theme.tooltip, trigger: "axis" },
    xAxis: horizontal
      ? { ...(theme as any).valueAxis, type: "value" as const }
      : { ...(theme as any).categoryAxis, type: "category" as const, data: xValues },
    yAxis: horizontal
      ? { ...(theme as any).categoryAxis, type: "category" as const, data: xValues }
      : { ...(theme as any).valueAxis, type: "value" as const },
    series: [{ type: "bar" as const, data: yValues, name: y }],
  };

  return (
    <div className="mt-2 mb-3">
      <ReactEChartsCore echarts={echarts} option={option} style={{ height: 291 }} />
    </div>
  );
}
