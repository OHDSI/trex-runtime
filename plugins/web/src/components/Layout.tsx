import { useSession, authClient } from "@/lib/auth-client";
import { Navigate, Outlet, Link } from "react-router-dom";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import { Button } from "@/components/ui/button";

export function Layout() {
  const { data: session, isPending } = useSession();

  if (isPending) {
    return (
      <div className="flex h-screen items-center justify-center">
        <div className="animate-spin h-8 w-8 border-4 border-primary border-t-transparent rounded-full" />
      </div>
    );
  }

  if (!session) {
    return <Navigate to="/login" replace />;
  }

  if ((session.user as any).mustChangePassword) {
    return <Navigate to="/change-password" replace />;
  }

  const initials = session.user?.name
    ?.split(" ")
    .map((n: string) => n[0])
    .join("")
    .toUpperCase() || "?";

  return (
    <div className="min-h-screen bg-background">
      <header className="border-b">
        <div className="container mx-auto flex h-14 items-center justify-between px-4">
          <Link to="/" className="font-semibold text-lg">
            TREX
          </Link>
          <div className="flex items-center gap-4">
            <a href={`${import.meta.env.BASE_URL}docs/`}
               className="text-sm text-muted-foreground hover:text-foreground transition-colors">
              Docs
            </a>
            {(session.user as any).role === "admin" && (
              <Link to="/admin">
                <Button variant="outline" size="sm">Admin</Button>
              </Link>
            )}
            <Link to="/profile">
              <span className="text-sm text-muted-foreground">
                {session.user.name}
              </span>
            </Link>
            <Avatar className="h-8 w-8">
              <AvatarImage src={session.user.image || undefined} />
              <AvatarFallback>{initials}</AvatarFallback>
            </Avatar>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => authClient.signOut()}
            >
              Sign out
            </Button>
          </div>
        </div>
      </header>
      <main className="container mx-auto px-4 py-6">
        <Outlet />
      </main>
    </div>
  );
}
