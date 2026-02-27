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
import { Switch } from "@/components/ui/switch";

const SETTINGS_QUERY = `
  query AuthSettings {
    allSettings(filter: { key: { startsWith: "auth." } }) {
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

export function AuthSettings() {
  const [result, reexecute] = useQuery({ query: SETTINGS_QUERY });
  const [, saveSetting] = useMutation(SAVE_SETTING_MUTATION);
  const [selfRegistration, setSelfRegistration] = useState(false);
  const [saving, setSaving] = useState(false);

  const settings: Record<string, any> = {};
  for (const node of result.data?.allSettings?.nodes || []) {
    settings[node.key] = node.value;
  }

  useEffect(() => {
    if (result.data) {
      setSelfRegistration(settings["auth.selfRegistration"] === true);
    }
  }, [result.data]);

  async function handleToggle(checked: boolean) {
    setSelfRegistration(checked);
    setSaving(true);
    try {
      const res = await saveSetting({ pKey: "auth.selfRegistration", pValue: checked });
      if (res.error) {
        toast.error(res.error.message);
        setSelfRegistration(!checked);
        return;
      }
      reexecute({ requestPolicy: "network-only" });
      toast.success("Setting saved.");
    } catch {
      toast.error("Failed to save setting.");
      setSelfRegistration(!checked);
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold">Auth Settings</h2>
        <p className="text-muted-foreground">
          Configure authentication behavior
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Self Registration</CardTitle>
          <CardDescription>
            Allow new users to create accounts via the registration page.
            When disabled, only admins can create user accounts.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-3">
            <Switch
              id="self-registration"
              checked={selfRegistration}
              onCheckedChange={handleToggle}
              disabled={saving || result.fetching}
            />
            <Label htmlFor="self-registration">
              {selfRegistration ? "Enabled" : "Disabled"}
            </Label>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
