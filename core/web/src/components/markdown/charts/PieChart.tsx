import ReactEChartsCore from "echarts-for-react/lib/core";
import * as echarts from "echarts/core";
import { PieChart as EPieChart } from "echarts/charts";
import {
  TooltipComponent,
  LegendComponent,
  TitleComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import { getEchartsTheme } from "../echarts-theme";
import { useQueryData } from "./QueryProvider";

echarts.use([
  EPieChart,
  TooltipComponent,
  LegendComponent,
  TitleComponent,
  CanvasRenderer,
]);

interface PieChartProps {
  name: string;
  value: string;
  title?: string;
  donut?: boolean;
  data?: string;
  query?: string;
}

export function PieChart({ name, value, title, donut, data, query }: PieChartProps) {
  const queryResult = useQueryData(query ?? "");
  const rows = data ? JSON.parse(data) : queryResult.data;
  const theme = getEchartsTheme();

  const seriesData = rows.map((r: Record<string, unknown>) => ({
    name: r[name],
    value: r[value],
  }));

  const option = {
    ...theme,
    title: title ? { ...theme.title, text: title } : undefined,
    tooltip: { ...theme.tooltip, trigger: "item" },
    legend: theme.legend,
    series: [
      {
        type: "pie" as const,
        radius: donut ? ["40%", "70%"] : "70%",
        data: seriesData,
        emphasis: {
          itemStyle: { shadowBlur: 10, shadowOffsetX: 0, shadowColor: "rgba(0,0,0,0.2)" },
        },
      },
    ],
  };

  return (
    <div className="mt-2 mb-3">
      <ReactEChartsCore echarts={echarts} option={option} style={{ height: 291 }} />
    </div>
  );
}
