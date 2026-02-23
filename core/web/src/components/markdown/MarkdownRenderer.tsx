import Markdown from "react-markdown";
import remarkGfm from "remark-gfm";
import rehypeRaw from "rehype-raw";
import rehypeSanitize, { defaultSchema } from "rehype-sanitize";

const sanitizeSchema = {
  ...defaultSchema,
  tagNames: [
    ...(defaultSchema.tagNames || []),
    "barchart", "linechart", "areachart", "piechart", "bigvalue", "datatable", "grid",
  ],
  attributes: {
    ...defaultSchema.attributes,
    "*": [...(defaultSchema.attributes?.["*"] || []), "className", "class"],
    barchart: ["query", "x", "y", "title", "height"],
    linechart: ["query", "x", "y", "title", "height"],
    areachart: ["query", "x", "y", "title", "height"],
    piechart: ["query", "name", "value", "title", "height"],
    bigvalue: ["query", "title", "format"],
    datatable: ["query", "title"],
    grid: ["columns"],
  },
};
import { QueryProvider } from "./charts/QueryProvider";
import { BarChart } from "./charts/BarChart";
import { LineChart } from "./charts/LineChart";
import { AreaChart } from "./charts/AreaChart";
import { PieChart } from "./charts/PieChart";
import { BigValue } from "./charts/BigValue";
import { MdDataTable } from "./charts/MdDataTable";
import { Grid } from "./charts/Grid";

// rehype-raw lowercases all HTML tag names
const components: Record<string, React.ComponentType<any>> = {
  barchart: BarChart,
  linechart: LineChart,
  areachart: AreaChart,
  piechart: PieChart,
  bigvalue: BigValue,
  datatable: MdDataTable,
  grid: Grid,

  h1: (props: any) => <h1 className="text-2xl tracking-wide font-bold mt-5 mb-1 first:mt-1 first:mb-2" {...props} />,
  h2: (props: any) => <h2 className="text-xl font-semibold mt-3 mb-1" {...props} />,
  h3: (props: any) => <h3 className="text-base font-semibold mt-2 mb-1" {...props} />,
  h4: (props: any) => <h4 className="text-sm font-semibold mt-1" {...props} />,
  p: (props: any) => <p className="text-base leading-normal mb-[1.2em]" {...props} />,
  ul: (props: any) => <ul className="list-disc pl-6 mb-[1.2em] space-y-1" {...props} />,
  ol: (props: any) => <ol className="list-decimal pl-6 mb-[1.2em] space-y-1" {...props} />,
  blockquote: (props: any) => (
    <blockquote className="border-l-4 border-border pl-4 italic text-muted-foreground mb-[1.2em]" {...props} />
  ),
  table: (props: any) => (
    <div className="text-sm mb-[1.2em] overflow-x-auto">
      <table className="w-full border-collapse tabular-nums" {...props} />
    </div>
  ),
  th: (props: any) => (
    <th className="text-left font-medium text-sm px-2 py-0.5 border-b border-muted-foreground/60 whitespace-nowrap" {...props} />
  ),
  td: (props: any) => (
    <td className="text-sm px-2 py-0.5 border-b border-muted-foreground/20 whitespace-nowrap overflow-hidden text-ellipsis" {...props} />
  ),
  code: ({ className, children, ...props }: any) => {
    const isInline = !className;
    if (isInline) {
      return (
        <code className="bg-muted px-1.5 py-0.5 rounded text-sm" {...props}>
          {children}
        </code>
      );
    }
    return (
      <code className={`block bg-muted p-4 rounded-md text-sm overflow-x-auto mb-4 ${className ?? ""}`} {...props}>
        {children}
      </code>
    );
  },
  pre: (props: any) => <pre className="mb-4" {...props} />,
  hr: () => <hr className="border-border my-6" />,
  a: (props: any) => <a className="text-primary underline underline-offset-4" {...props} />,
};

interface MarkdownRendererProps {
  content: string;
}

export function MarkdownRenderer({ content }: MarkdownRendererProps) {
  return (
    <QueryProvider queries={{}}>
      <div className="max-w-7xl mx-auto font-sans antialiased">
        <Markdown
          remarkPlugins={[remarkGfm]}
          rehypePlugins={[rehypeRaw, [rehypeSanitize, sanitizeSchema]]}
          components={components}
        >
          {content}
        </Markdown>
      </div>
    </QueryProvider>
  );
}
