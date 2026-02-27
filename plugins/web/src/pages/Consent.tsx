import { useState } from "react";
import { useSearchParams } from "react-router-dom";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { BASE_PATH } from "@/lib/config";
import { toast } from "sonner";

const SCOPE_DESCRIPTIONS: Record<string, string> = {
  openid: "Verify your identity",
  profile: "Access your name and profile information",
  email: "Access your email address",
  offline_access: "Maintain access when you are not actively using the app",
};

export function Consent() {
  const [searchParams] = useSearchParams();
  const [submitting, setSubmitting] = useState(false);

  const clientId = searchParams.get("client_id") || "";
  const clientName = searchParams.get("client_name") || clientId || "Unknown Application";
  const scopeParam = searchParams.get("scope") || "openid";
  const redirectUri = searchParams.get("redirect_uri") || "";
  const responseType = searchParams.get("response_type") || "code";
  const state = searchParams.get("state") || "";

  const scopes = scopeParam.split(" ").filter(Boolean);

  async function handleAuthorize() {
    setSubmitting(true);
    try {
      const res = await fetch(`${BASE_PATH}/api/auth/oauth2/consent`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({
          clientId,
          scopes,
          redirectUri,
          responseType,
          state,
          accept: true,
        }),
      });

      const data = await res.json();
      if (data?.redirectTo) {
        try {
          const url = new URL(data.redirectTo);
          if (url.protocol !== "https:" && url.protocol !== "http:") {
            toast.error("Invalid redirect URI");
            return;
          }
        } catch {
          toast.error("Invalid redirect URI");
          return;
        }
        window.location.href = data.redirectTo;
      } else if (!res.ok) {
        toast.error(data?.message || "Authorization failed");
      }
    } catch {
      toast.error("An error occurred during authorization");
    } finally {
      setSubmitting(false);
    }
  }

  function handleDeny() {
    if (!redirectUri) {
      toast.error("No redirect URI provided");
      return;
    }
    try {
      const denyUrl = new URL(redirectUri);
      // Only allow http(s) schemes to prevent javascript: or data: redirects
      if (denyUrl.protocol !== "https:" && denyUrl.protocol !== "http:") {
        toast.error("Invalid redirect URI");
        return;
      }
      denyUrl.searchParams.set("error", "access_denied");
      denyUrl.searchParams.set("error_description", "The user denied the authorization request");
      if (state) {
        denyUrl.searchParams.set("state", state);
      }
      window.location.href = denyUrl.toString();
    } catch {
      toast.error("Invalid redirect URI");
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-background p-4">
      <Card className="w-full max-w-md">
        <CardHeader className="text-center">
          <CardTitle className="text-2xl">Authorization Request</CardTitle>
          <CardDescription>
            <span className="font-semibold text-foreground">{clientName}</span>{" "}
            is requesting access to your account
          </CardDescription>
        </CardHeader>

        <CardContent className="space-y-4">
          <div>
            <p className="text-sm font-medium text-muted-foreground mb-3">
              This application is requesting the following permissions:
            </p>
            <div className="space-y-2">
              {scopes.map((scope) => (
                <div key={scope} className="flex items-center gap-2">
                  <Badge variant="secondary">{scope}</Badge>
                  <span className="text-sm text-muted-foreground">
                    {SCOPE_DESCRIPTIONS[scope] || scope}
                  </span>
                </div>
              ))}
            </div>
          </div>

          <Separator />

          <p className="text-xs text-muted-foreground">
            By authorizing, you allow this application to access the
            requested information. You can revoke access at any time from
            your profile settings.
          </p>
        </CardContent>

        <CardFooter className="flex gap-3">
          <Button
            variant="outline"
            className="flex-1"
            onClick={handleDeny}
            disabled={submitting}
          >
            Deny
          </Button>
          <Button
            className="flex-1"
            onClick={handleAuthorize}
            disabled={submitting}
          >
            {submitting ? "Authorizing..." : "Authorize"}
          </Button>
        </CardFooter>
      </Card>
    </div>
  );
}
