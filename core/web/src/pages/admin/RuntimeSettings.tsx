import { useEffect, useState } from "react";
import { useQuery, useMutation } from "urql";
import { toast } from "sonner";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Label } from "@/components/ui/label";

const SETTINGS_QUERY = `
  query RuntimeSettings {
    allSettings(filter: { key: { startsWith: "runtime." } }) {
      nodes { key value }
    }
  }
`;

const SAVE_SETTING_MUTATION = `
  mutation SaveSetting($pKey: String!, $pValue: JSON!) {
    saveSetting(input: { pKey: $pKey, pValue: $pValue }) {
      setting { key value }
    }
  }
`;

const LOGGING_OPTIONS = [
  { value: "console", label: "Console" },
  { value: "database", label: "Database" },
  { value: "both", label: "Both" },
];

export function RuntimeSettings() {
  const [result, reexecute] = useQuery({ query: SETTINGS_QUERY });
  const [, saveSetting] = useMutation(SAVE_SETTING_MUTATION);
  const [functionLogging, setFunctionLogging] = useState("console");
  const [saving, setSaving] = useState(false);

  const settings: Record<string, any> = {};
  for (const node of result.data?.allSettings?.nodes || []) {
    settings[node.key] = node.value;
  }

  useEffect(() => {
    if (result.data) {
      setFunctionLogging(settings["runtime.functionLogging"] || "console");
    }
  }, [result.data]);

  async function handleLoggingChange(value: string) {
    const prev = functionLogging;
    setFunctionLogging(value);
    setSaving(true);
    try {
      const res = await saveSetting({ pKey: "runtime.functionLogging", pValue: value });
      if (res.error) {
        toast.error(res.error.message);
        setFunctionLogging(prev);
        return;
      }
      reexecute({ requestPolicy: "network-only" });
      toast.success("Setting saved.");
    } catch {
      toast.error("Failed to save setting.");
      setFunctionLogging(prev);
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold">Runtime Settings</h2>
        <p className="text-muted-foreground">
          Configure runtime behavior
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Function Logging</CardTitle>
          <CardDescription>
            Control where function worker logs are sent.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex flex-col gap-2 max-w-xs">
            <Label htmlFor="function-logging">Log destination</Label>
            <select
              id="function-logging"
              className="border-input bg-transparent flex h-9 w-full rounded-md border px-3 py-1 text-sm focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px] outline-none"
              value={functionLogging}
              onChange={(e) => handleLoggingChange(e.target.value)}
              disabled={saving || result.fetching}
            >
              {LOGGING_OPTIONS.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </select>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
