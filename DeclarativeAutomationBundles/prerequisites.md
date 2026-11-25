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

# 3. Create the Catalog and Medallion Schemas

Create the catalog for the target (`databricksproject_ws` for dev, `databricksproject_ws_prod` for prod) with the schemas `01_bronze`, `02_silver` and `03_gold`. The governance task creates `04_governance` and `05_secure` itself.

The identity that runs `master_job` (the deploying user or the CI service principal) must own the catalog or hold `MANAGE` on it, so it can create functions, grant privileges and create policies.

---

# 4. Create the Access Groups

Create these account-level groups and add members:

| Group | Access |
|-------|--------|
| `restaurant_owners` | All layers, enterprise-wide, PII unmasked; manages restaurant access |
| `data_engineers` | Full access to Bronze, Silver and Gold; PII masked in Gold |
| `restaurant_analysts` | Secure views and row-filtered Gold only; restaurant-scoped, PII masked |

The group names are bundle variables (`owners_group`, `engineers_group`, `analysts_group`) in `databricks.yml`.

---

# 5. Create the `pii` Governed Tag (for ABAC)

The ABAC masking policy matches columns tagged `pii`. In the account console, create a governed tag named **`pii`** with the allowed values `name`, `email` and `phone`. ABAC must be enabled for the workspace.

---

# 6. Grant Restaurant Access to Analysts

Analysts see only the restaurants mapped to them. Owners maintain the mapping:

```sql
INSERT INTO databricksproject_ws.`04_governance`.restaurant_access (principal, restaurant_id)
VALUES ('analyst@example.com', 'R001');
```

---

# 7. Deploy the Bundle

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
Then run the job once to create the governance objects and load data:

```bash
databricks bundle run master_job --target dev
```

---

# 8. CI/CD with GitHub Actions

`.github/workflows/databricks-bundle.yml` validates the bundle on every pull request to `master` and deploys the prod target on every push to `master`. Add these repository secrets for a service principal with access to the workspace:

| Secret | Value |
|--------|-------|
| `DATABRICKS_HOST` | Workspace URL |
| `DATABRICKS_CLIENT_ID` | Service principal application ID |
| `DATABRICKS_CLIENT_SECRET` | Service principal OAuth secret |
