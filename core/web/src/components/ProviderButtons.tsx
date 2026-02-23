import { useState } from "react";
import { useQuery } from "urql";
import { authClient } from "@/lib/auth-client";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";

const ENABLED_SSO_PROVIDERS_QUERY = `
  query EnabledSsoProviders {
    enabledSsoProviders {
      nodes { id displayName }
    }
  }
`;

export function ProviderButtons() {
  const [loadingProvider, setLoadingProvider] = useState<string | null>(null);

  const [result] = useQuery({ query: ENABLED_SSO_PROVIDERS_QUERY });

  const providers: { id: string; displayName: string }[] =
    result.data?.enabledSsoProviders?.nodes ?? [];

  if (result.fetching || providers.length === 0) {
    return null;
  }

  async function handleSocialSignIn(provider: string) {
    setLoadingProvider(provider);
    try {
      await authClient.signIn.social({
        provider: provider as any,
        callbackURL: "/",
      });
    } catch {
      setLoadingProvider(null);
    }
  }

  return (
    <>
      <div className="relative my-4 flex items-center">
        <Separator className="flex-1" />
        <span className="mx-4 text-xs text-muted-foreground uppercase">
          or continue with
        </span>
        <Separator className="flex-1" />
      </div>
      <div className="flex flex-col gap-2">
        {providers.map(({ id, displayName }) => (
          <Button
            key={id}
            variant="outline"
            className="w-full"
            disabled={loadingProvider !== null}
            onClick={() => handleSocialSignIn(id)}
          >
            {loadingProvider === id
              ? "Redirecting..."
              : `Sign in with ${displayName}`}
          </Button>
        ))}
      </div>
    </>
  );
}
