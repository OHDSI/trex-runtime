import {
  Table,
  TableHeader,
  TableBody,
  TableHead,
  TableRow,
  TableCell,
} from "@/components/ui/table";
import { useQueryData } from "./QueryProvider";

interface MdDataTableProps {
  columns?: string;
  data?: string;
  query?: string;
}

export function MdDataTable({ columns, data, query }: MdDataTableProps) {
  const queryResult = useQueryData(query ?? "");
  const rows: Record<string, unknown>[] = data ? JSON.parse(data) : queryResult.data;

  const cols = columns
    ? columns.split(",").map((c) => c.trim())
    : rows.length > 0
      ? Object.keys(rows[0])
      : [];

  if (rows.length === 0) return null;

  return (
    <div className="rounded-md border text-sm">
      <Table>
        <TableHeader>
          <TableRow>
            {cols.map((col) => (
              <TableHead key={col}>{col}</TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((row, i) => (
            <TableRow key={i}>
              {cols.map((col) => (
                <TableCell key={col}>{String(row[col] ?? "")}</TableCell>
              ))}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}
