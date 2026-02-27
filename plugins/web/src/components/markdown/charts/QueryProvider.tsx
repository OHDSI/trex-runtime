import { createContext, useContext, type ReactNode } from "react";

interface QueryEntry {
  data: Record<string, unknown>[];
  loading: boolean;
}

type QueryStore = Record<string, QueryEntry>;

const QueryContext = createContext<QueryStore>({});

export function QueryProvider({
  queries,
  children,
}: {
  queries: QueryStore;
  children: ReactNode;
}) {
  return (
    <QueryContext.Provider value={queries}>{children}</QueryContext.Provider>
  );
}

export function useQueryData(name: string): QueryEntry {
  const store = useContext(QueryContext);
  return store[name] ?? { data: [], loading: false };
}
