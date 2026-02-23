import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

const sidebars: SidebarsConfig = {
  docsSidebar: [
    "intro",
    {
      type: "category",
      label: "SQL Reference",
      items: [
        "sql-reference/db",
        "sql-reference/tpm",
        "sql-reference/hana",
        "sql-reference/pgwire",
        "sql-reference/chdb",
        "sql-reference/etl",
        "sql-reference/fhir",
        "sql-reference/migration",
        "sql-reference/ai",
        "sql-reference/atlas",
      ],
    },
    {
      type: "category",
      label: "Plugins",
      items: [
        "plugins/overview",
        "plugins/developing",
        "plugins/ui-plugins",
        "plugins/function-plugins",
        "plugins/flow-plugins",
        "plugins/migration-plugins",
        "plugins/transform-plugins",
      ],
    },
    {
      type: "category",
      label: "JS / API Interface",
      items: [
        "js-interface/graphql",
        "js-interface/auth",
        "js-interface/mcp",
        "js-interface/functions",
      ],
    },
    {
      type: "category",
      label: "Deployment",
      items: [
        "deployment/docker",
        "deployment/environment",
      ],
    },
  ],
};

export default sidebars;
