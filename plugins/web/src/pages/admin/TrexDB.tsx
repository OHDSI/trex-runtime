import { useState, useEffect } from "react";
import { useQuery } from "urql";
import { DataTable, type Column } from "@/components/DataTable";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { RefreshCwIcon } from "lucide-react";

const TREX_DATABASES_QUERY = `
  query TrexDatabases {
    trexDatabases { databaseName databaseOid path internal type }
  }
`;

const TREX_SCHEMAS_QUERY = `
  query TrexSchemas {
    trexSchemas { databaseName schemaName schemaOid internal }
  }
`;

const TREX_TABLES_QUERY = `
  query TrexTables {
    trexTables { databaseName schemaName tableName }
  }
`;

interface DatabaseRow {
  databaseName: string;
  databaseOid: string;
  path: string | null;
  internal: boolean;
  type: string;
}

interface SchemaRow {
  databaseName: string;
  schemaName: string;
  schemaOid: string;
  internal: boolean;
}

interface TableRow {
  databaseName: string;
  schemaName: string;
  tableName: string;
}

export function TrexDB() {
  const [selectedDatabase, setSelectedDatabase] = useState<string | null>(null);

  const [dbResult, reexecuteDbs] = useQuery({ query: TREX_DATABASES_QUERY });
  const [schemaResult, reexecuteSchemas] = useQuery({ query: TREX_SCHEMAS_QUERY });
  const [tableResult, reexecuteTables] = useQuery({ query: TREX_TABLES_QUERY });

  const databases: DatabaseRow[] = dbResult.data?.trexDatabases || [];
  const allSchemas: SchemaRow[] = schemaResult.data?.trexSchemas || [];
  const allTables: TableRow[] = tableResult.data?.trexTables || [];

  const schemas = selectedDatabase
    ? allSchemas.filter((s) => s.databaseName === selectedDatabase)
    : allSchemas;

  const schemaCountByDb = new Map<string, number>();
  for (const s of allSchemas) {
    schemaCountByDb.set(s.databaseName, (schemaCountByDb.get(s.databaseName) || 0) + 1);
  }

  const tableCountBySchema = new Map<string, number>();
  for (const t of allTables) {
    const key = `${t.databaseName}.${t.schemaName}`;
    tableCountBySchema.set(key, (tableCountBySchema.get(key) || 0) + 1);
  }

  function refetchAll() {
    reexecuteDbs({ requestPolicy: "network-only" });
    reexecuteSchemas({ requestPolicy: "network-only" });
    reexecuteTables({ requestPolicy: "network-only" });
  }

  useEffect(() => {
    const interval = setInterval(refetchAll, 30000);
    return () => clearInterval(interval);
  }, []);

  const databaseColumns: Column<DatabaseRow>[] = [
    {
      header: "Name",
      cell: (row) => <code className="text-xs font-mono">{row.databaseName}</code>,
    },
    {
      header: "Type",
      cell: (row) => <Badge variant="secondary">{row.type}</Badge>,
    },
    {
      header: "Path",
      cell: (row) => (
        <span className="text-sm text-muted-foreground">{row.path || "-"}</span>
      ),
    },
    {
      header: "Schemas",
      cell: (row) => (
        <span className="text-sm">{schemaCountByDb.get(row.databaseName) || 0}</span>
      ),
    },
    {
      header: "Internal",
      cell: (row) => (
        <Badge variant={row.internal ? "secondary" : "default"}>
          {row.internal ? "Yes" : "No"}
        </Badge>
      ),
    },
  ];

  const schemaColumns: Column<SchemaRow>[] = [
    {
      header: "Database",
      cell: (row) => <code className="text-xs font-mono">{row.databaseName}</code>,
    },
    {
      header: "Schema",
      cell: (row) => <span className="text-sm font-medium">{row.schemaName}</span>,
    },
    {
      header: "Tables",
      cell: (row) => (
        <span className="text-sm">{tableCountBySchema.get(`${row.databaseName}.${row.schemaName}`) || 0}</span>
      ),
    },
    {
      header: "Internal",
      cell: (row) => (
        <Badge variant={row.internal ? "secondary" : "default"}>
          {row.internal ? "Yes" : "No"}
        </Badge>
      ),
    },
  ];

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-2xl font-bold">Databases</h2>
          <p className="text-muted-foreground">
            {selectedDatabase
              ? `Browsing database: ${selectedDatabase}`
              : `${databases.length} database${databases.length === 1 ? "" : "s"} attached`}
          </p>
        </div>
        <div className="flex gap-2">
          {selectedDatabase && (
            <Button variant="outline" onClick={() => setSelectedDatabase(null)}>
              Clear Filter
            </Button>
          )}
          <Button variant="outline" onClick={refetchAll}>
            <RefreshCwIcon className="h-4 w-4" />
          </Button>
        </div>
      </div>

      <div className="space-y-2">
        <h3 className="text-lg font-semibold">Databases</h3>
        <DataTable
          columns={databaseColumns}
          data={databases}
          loading={dbResult.fetching}
          onRowClick={(row) =>
            setSelectedDatabase(
              row.databaseName === selectedDatabase ? null : row.databaseName
            )
          }
          emptyMessage="No databases found."
        />
      </div>

      <div className="space-y-2">
        <h3 className="text-lg font-semibold">Schemas</h3>
        <DataTable
          columns={schemaColumns}
          data={schemas}
          loading={schemaResult.fetching}
          emptyMessage="No schemas found."
        />
      </div>
    </div>
  );
}
