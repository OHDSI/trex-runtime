import ReactEChartsCore from "echarts-for-react/lib/core";
import * as echarts from "echarts/core";
import { LineChart } from "echarts/charts";
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
  LineChart,
  GridComponent,
  TooltipComponent,
  LegendComponent,
  TitleComponent,
  CanvasRenderer,
]);

interface AreaChartProps {
  x: string;
  y: string;
  title?: string;
  data?: string;
  query?: string;
}

export function AreaChart({ x, y, title, data, query }: AreaChartProps) {
  const queryResult = useQueryData(query ?? "");
  const rows = data ? JSON.parse(data) : queryResult.data;
  const theme = getEchartsTheme();

  const xValues = rows.map((r: Record<string, unknown>) => r[x]);
  const yValues = rows.map((r: Record<string, unknown>) => r[y]);

  const option = {
    ...theme,
    title: title ? { ...theme.title, text: title } : undefined,
    tooltip: { ...theme.tooltip, trigger: "axis" },
    xAxis: { ...(theme as any).categoryAxis, type: "category" as const, data: xValues },
    yAxis: { ...(theme as any).valueAxis, type: "value" as const },
    series: [
      {
        type: "line" as const,
        data: yValues,
        name: y,
        areaStyle: {},
        symbolSize: 0,
      },
    ],
  };

  return (
    <div className="mt-2 mb-3">
      <ReactEChartsCore echarts={echarts} option={option} style={{ height: 291 }} />
    </div>
  );
}
