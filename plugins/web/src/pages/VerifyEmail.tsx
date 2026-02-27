import { useState, useEffect } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { authClient } from "@/lib/auth-client";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

export function VerifyEmail() {
  const [searchParams] = useSearchParams();
  const token = searchParams.get("token");

  const [status, setStatus] = useState<"loading" | "success" | "error">(
    token ? "loading" : "error"
  );
  const [errorMessage, setErrorMessage] = useState(
    token ? "" : "This verification link is invalid or has expired."
  );

  useEffect(() => {
    if (!token) return;

    let cancelled = false;

    async function verify() {
      try {
        const result = await authClient.verifyEmail({ query: { token: token! } });

        if (cancelled) return;

        if (result.error) {
          setErrorMessage(
            result.error.message || "Email verification failed."
          );
          setStatus("error");
        } else {
          setStatus("success");
        }
      } catch {
        if (cancelled) return;
        setErrorMessage("An unexpected error occurred. Please try again.");
        setStatus("error");
      }
    }

    verify();

    return () => {
      cancelled = true;
    };
  }, [token]);

  return (
    <div className="flex min-h-screen items-center justify-center px-4">
      <Card className="w-full max-w-sm">
        {status === "loading" && (
          <CardHeader className="text-center">
            <CardTitle className="text-2xl">Verifying Email</CardTitle>
            <CardDescription>
              <span className="flex items-center justify-center gap-2">
                <span className="animate-spin h-4 w-4 border-2 border-primary border-t-transparent rounded-full" />
                Please wait while we verify your email address...
              </span>
            </CardDescription>
          </CardHeader>
        )}

        {status === "success" && (
          <>
            <CardHeader className="text-center">
              <CardTitle className="text-2xl">Email Verified</CardTitle>
              <CardDescription>
                Your email address has been verified successfully. You can now
                sign in to your account.
              </CardDescription>
            </CardHeader>
            <CardFooter className="justify-center">
              <Button asChild>
                <Link to="/login">Sign In</Link>
              </Button>
            </CardFooter>
          </>
        )}

        {status === "error" && (
          <>
            <CardHeader className="text-center">
              <CardTitle className="text-2xl">Verification Failed</CardTitle>
              <CardDescription>{errorMessage}</CardDescription>
            </CardHeader>
            <CardFooter className="justify-center">
              <Link
                to="/login"
                className="text-sm text-foreground underline underline-offset-4 hover:text-foreground/80"
              >
                Back to sign in
              </Link>
            </CardFooter>
          </>
        )}
      </Card>
    </div>
  );
}
