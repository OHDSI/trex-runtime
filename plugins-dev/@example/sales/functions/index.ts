Deno.serve(async (req: Request) => {
  const url = new URL(req.url);
  const path = url.pathname;

  if (path.endsWith("/health")) {
    return Response.json({ status: "ok", plugin: "@example/sales", ts: new Date().toISOString() });
  }

  if (path.endsWith("/summary")) {
    return Response.json({
      plugin: "@example/sales",
      version: "1.0.0",
      pluginTypes: ["functions", "ui", "migrations", "transform", "flow"],
      seeds: ["products", "orders"],
      models: {
        staging: ["stg_products", "stg_orders"],
        marts: ["fct_revenue_by_product", "fct_daily_revenue"],
      },
      endpoints: {
        dashboard: "/plugins/example/sales/",
        api: "/plugins/example/sales-api/",
        revenueByProduct: "/plugins/transform/sales/revenue-by-product",
        dailyRevenue: "/plugins/transform/sales/daily-revenue",
      },
    });
  }

  if (path.endsWith("/regions")) {
    const regions = [
      { name: "North America", code: "NA" },
      { name: "Europe", code: "EU" },
      { name: "Asia", code: "APAC" },
    ];
    return Response.json(regions);
  }

  return Response.json({ error: "Not found", availableEndpoints: ["/health", "/summary", "/regions"] }, { status: 404 });
});
