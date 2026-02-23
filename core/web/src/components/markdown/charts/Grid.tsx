import type { ReactNode } from "react";

interface GridProps {
  cols?: string;
  children?: ReactNode;
}

export function Grid({ cols = "3", children }: GridProps) {
  const safeCols = /^\d{1,2}$/.test(cols) ? cols : "3";
  return (
    <div
      className="grid gap-4"
      style={{ gridTemplateColumns: `repeat(${safeCols}, minmax(0, 1fr))` }}
    >
      {children}
    </div>
  );
}
