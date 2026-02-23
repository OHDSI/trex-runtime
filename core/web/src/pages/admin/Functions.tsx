import { useEffect, useMemo, useState } from "react";
import { useQuery } from "urql";
import { DataTable, type Column } from "@/components/DataTable";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { RefreshCwIcon } from "lucide-react";

const REGISTERED_FUNCTIONS_QUERY = `
  query RegisteredFunctions {
    registeredFunctions { pluginName source entryPoint }
    roleScopeMappings { role scopes }
    urlScopeRequirements { path scopes }
  }
`;

interface FunctionRow {
  pluginName: string;
  source: string;
  entryPoint: string;
}

interface RoleScopeRow {
  role: string;
  scopes: string[];
}

interface UrlScopeRow {
  path: string;
  scopes: string[];
}

function truncate(value: string, max = 40): string {
  return value.length > max ? value.slice(0, max) + "\u2026" : value;
}

export function Functions() {
  const [result, reexecute] = useQuery({ query: REGISTERED_FUNCTIONS_QUERY });
  const [selectedScope, setSelectedScope] = useState("");

  const functions: FunctionRow[] = result.data?.registeredFunctions || [];
  const roleMappings: RoleScopeRow[] = result.data?.roleScopeMappings || [];
  const urlScopes: UrlScopeRow[] = result.data?.urlScopeRequirements || [];

  // Collect all unique scopes from both roles and URL requirements
  const allScopes = useMemo(() => {
    const set = new Set<string>();
    for (const r of roleMappings) r.scopes.forEach((s) => set.add(s));
    for (const u of urlScopes) u.scopes.forEach((s) => set.add(s));
    return Array.from(set).sort();
  }, [roleMappings, urlScopes]);

  // Filter by selected scope
  const filteredRoleMappings = useMemo(
    () => selectedScope ? roleMappings.filter((r) => r.scopes.includes(selectedScope)) : roleMappings,
    [roleMappings, selectedScope]
  );
  const filteredUrlScopes = useMemo(
    () => selectedScope ? urlScopes.filter((u) => u.scopes.includes(selectedScope)) : urlScopes,
    [urlScopes, selectedScope]
  );

  function refetch() {
    reexecute({ requestPolicy: "network-only" });
  }

  useEffect(() => {
    const interval = setInterval(refetch, 30000);
    return () => clearInterval(interval);
  }, []);

  const fnColumns: Column<FunctionRow>[] = [
    {
      header: "Plugin",
      cell: (row) => <span className="text-sm font-medium">{row.pluginName}</span>,
    },
    {
      header: "Source",
      cell: (row) => (
        <code className="text-xs font-mono" title={row.source}>
          {truncate(row.source)}
        </code>
      ),
    },
    {
      header: "Entry Point",
      cell: (row) => (
        <code className="text-xs font-mono" title={row.entryPoint}>
          {truncate(row.entryPoint)}
        </code>
      ),
    },
  ];

  const roleColumns: Column<RoleScopeRow>[] = [
    {
      header: "Role",
      cell: (row) => <span className="text-sm font-medium">{row.role}</span>,
    },
    {
      header: "Scopes",
      cell: (row) => (
        <div className="flex flex-wrap gap-1">
          {row.scopes.map((s) => (
            <Badge
              key={s}
              variant={selectedScope === s ? "default" : "secondary"}
              className="cursor-pointer"
              onClick={() => setSelectedScope(selectedScope === s ? "" : s)}
            >
              {s}
            </Badge>
          ))}
        </div>
      ),
    },
  ];

  const urlColumns: Column<UrlScopeRow>[] = [
    {
      header: "URL Pattern",
      cell: (row) => (
        <code className="text-xs font-mono" title={row.path}>
          {truncate(row.path, 60)}
        </code>
      ),
    },
    {
      header: "Required Scopes",
      cell: (row) => (
        <div className="flex flex-wrap gap-1">
          {row.scopes.map((s) => (
            <Badge
              key={s}
              variant={selectedScope === s ? "default" : "secondary"}
              className="cursor-pointer"
              onClick={() => setSelectedScope(selectedScope === s ? "" : s)}
            >
              {s}
            </Badge>
          ))}
        </div>
      ),
    },
  ];

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-2xl font-bold">Functions</h2>
          <p className="text-muted-foreground">
            {functions.length} registered function{functions.length === 1 ? "" : "s"}
          </p>
        </div>
        <div className="flex items-center gap-2">
          {allScopes.length > 0 && (
            <select
              className="border-input bg-transparent flex h-9 rounded-md border px-3 py-1 text-sm focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px] outline-none"
              value={selectedScope}
              onChange={(e) => setSelectedScope(e.target.value)}
            >
              <option value="">All scopes</option>
              {allScopes.map((s) => (
                <option key={s} value={s}>{s}</option>
              ))}
            </select>
          )}
          <Button variant="outline" onClick={refetch}>
            <RefreshCwIcon className="h-4 w-4" />
          </Button>
        </div>
      </div>

      <div className="space-y-2">
        <h3 className="text-lg font-semibold">Registered Functions</h3>
        <DataTable
          columns={fnColumns}
          data={functions}
          loading={result.fetching}
          emptyMessage="No functions registered."
        />
      </div>

      <div className="space-y-2">
        <h3 className="text-lg font-semibold">Role-Scope Mappings</h3>
        <DataTable
          columns={roleColumns}
          data={filteredRoleMappings}
          loading={result.fetching}
          emptyMessage={selectedScope ? `No roles with scope "${selectedScope}".` : "No role-scope mappings configured."}
        />
      </div>

      <div className="space-y-2">
        <h3 className="text-lg font-semibold">URL Scope Requirements</h3>
        <DataTable
          columns={urlColumns}
          data={filteredUrlScopes}
          loading={result.fetching}
          emptyMessage={selectedScope ? `No URLs requiring scope "${selectedScope}".` : "No URL scope requirements configured."}
        />
      </div>
    </div>
  );
}
