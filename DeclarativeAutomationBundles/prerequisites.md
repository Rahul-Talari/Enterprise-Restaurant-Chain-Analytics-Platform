# Prerequisites

Before deploying this Databricks Asset Bundle (DAB), complete the following one-time setup for each Databricks workspace.

---

# 1. Create the Unity Catalog Connection (Azure SQL Database)

Create a Unity Catalog Connection named:

- **Name:** `conn_restaurantops`
- **Type:** `SQLSERVER`

Example:

```bash
databricks connections create --json '{
  "name": "conn_restaurantops",
  "connection_type": "SQLSERVER",
  "options": {
    "host": "sqlserverops-databricksproject.database.windows.net",
    "port": "1433",
    "user": "<SQL_USERNAME>",
    "password": "<SQL_PASSWORD>"
  }
}'
```

> Store credentials securely using your organization's secret management solution. Never commit credentials to source control.

Verify the connection:

```bash
databricks connections list
```

---

# 2. Create the Databricks Secret Scope (Azure Event Hubs)

Create a Databricks Secret Scope:

```text
restaurantproject
```

Create the following secrets within the scope:

| Secret Key | Description |
|------------|-------------|
| `EVENTHUB_NAMESPACE` | Azure Event Hubs namespace |
| `EVENTHUB_NAME` | Azure Event Hub name |
| `EVENTHUB_CONN_STR` | Azure Event Hubs connection string |

> The Event Hubs ingestion pipeline reads these secrets at runtime using `dbutils.secrets.get()`.

---

# 3. Deploy the Bundle

Deploy to the Development environment:

```bash
databricks bundle validate
databricks bundle deploy --target dev
```

Deploy to the Production environment:

```bash
databricks bundle validate
databricks bundle deploy --target prod
```