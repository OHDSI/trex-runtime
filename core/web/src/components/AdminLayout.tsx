import { useSession } from "@/lib/auth-client";
import { Outlet, NavLink } from "react-router-dom";
import { Separator } from "@/components/ui/separator";

export function AdminLayout() {
  const { data: session, isPending } = useSession();

  if (isPending) return null;

  if (!session?.user || (session.user as any).role !== "admin") {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="text-center">
          <h2 className="text-2xl font-bold">403 Forbidden</h2>
          <p className="text-muted-foreground mt-2">
            You do not have permission to access this page.
          </p>
        </div>
      </div>
    );
  }

  const navSections = [
    {
      label: "Runtime",
      items: [
        { to: "/admin/extensions", label: "Extensions" },
        { to: "/admin/plugins", label: "Plugins" },
        { to: "/admin/services", label: "Services" },
        { to: "/admin/runtime-settings", label: "Settings" },
        { to: "/admin/functions", label: "Functions" },
        { to: "/admin/flows", label: "Flows" },
        { to: "/admin/ui", label: "UI" },
        { to: "/admin/logs", label: "Logs" },
      ],
    },
    {
      label: "Database",
      items: [
        { to: "/admin/trexdb", label: "Databases" },
        { to: "/admin/migrations", label: "Migrations" },
        { to: "/admin/databases", label: "Federation" },
        { to: "/admin/etl", label: "Replication" },
        { to: "/admin/transforms", label: "Transforms" },
        { to: "/admin/subscriptions", label: "Subscriptions" },
      ],
    },
    {
      label: "Analytics",
      items: [
        { to: "/admin/analytics", label: "Dashboard" },
      ],
    },
    {
      label: "Auth",
      items: [
        { to: "/admin/users", label: "Users" },
        { to: "/admin/roles", label: "Roles" },
        { to: "/admin/sessions", label: "Sessions" },
        { to: "/admin/sso", label: "SSO Providers" },
        { to: "/admin/apps", label: "Applications" },
        { to: "/admin/auth-settings", label: "Settings" },
      ],
    },
  ];

  return (
    <div className="flex gap-6">
      <aside className="w-48 shrink-0">
        <nav className="flex flex-col gap-4">
          {navSections.map((section) => (
            <div key={section.label} className="flex flex-col gap-1">
              <span className="px-3 text-xs font-semibold uppercase tracking-wider text-muted-foreground/60">
                {section.label}
              </span>
              {section.items.map((item) => (
                <NavLink
                  key={item.to}
                  to={item.to}
                  className={({ isActive }) =>
                    `px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                      isActive
                        ? "bg-accent text-accent-foreground"
                        : "text-muted-foreground hover:bg-accent/50"
                    }`
                  }
                >
                  {item.label}
                </NavLink>
              ))}
            </div>
          ))}
        </nav>
      </aside>
      <Separator orientation="vertical" className="h-auto" />
      <div className="flex-1 min-w-0">
        <Outlet />
      </div>
    </div>
  );
}
