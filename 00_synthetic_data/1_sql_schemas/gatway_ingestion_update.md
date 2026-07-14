# Handling Lakeflow Connect Compute Quota Issues

There are two approaches to prevent Lakeflow Connect from provisioning larger clusters or to resolve Azure compute quota limitations.

---

## Way 1: Restrict Compute Using a Compute Policy

By default, **Lakeflow Connect** automatically creates an **all-purpose job compute** for the ingestion gateway. Depending on the workload, it may provision larger clusters than required.

To control the compute configuration, create a **Custom Compute Policy** and attach it to the **Lakeflow Connect pipeline**.

> **Note:** The compute policy cannot be attached directly to the compute because Lakeflow Connect recreates the compute automatically. Instead, it must be associated with the pipeline definition.

### Step 1: Create a Custom Compute Policy

Navigate to:

**Compute → Policies → Create Policy**

Configure the policy as follows:

| Property | Value |
|----------|-------|
| **Name** | `<policy-name>` |
| **Family** | `Custom` |
| **Description** | Restrict Lakeflow Connect to use minimal compute resources. |

### Policy Definitions

| Field | Type | Value |
|-------|------|-------|
| `num_workers` | Fixed | `1` |
| `driver_node_type_id` | Fixed | `<Driver node type created by Lakeflow Connect>` |
| `node_type_id` | Fixed | `<Worker node type created by Lakeflow Connect>` |

Save the policy and copy the **Policy ID**.

---

### Step 2: Install and Configure the Databricks CLI

Verify the CLI installation:

```bash
databricks --version
```

Configure the CLI:

```bash
databricks configure
```

Provide:

- **Workspace Host**

```text
https://adb-xxxxxxxxxxxxxxxx.x.azuredatabricks.net
```

- **Personal Access Token (PAT)**

Generate from:

```
Workspace
└── Settings
    └── Developer
        └── Access Tokens
            └── Generate New Token
```

Copy and paste the generated token when prompted.

---

### Step 3: List Existing Pipelines

```bash
databricks pipelines list-pipelines --output json
```

Locate the **Gateway Ingestion** pipeline and copy its `pipeline_id`.

---

### Step 4: Update the Pipeline

Replace the placeholders with your values and run:

```bash
databricks pipelines update <pipeline_id> --json '{
  "name": "gw_ingestion_silver",
  "catalog": "<catalog_name>",
  "schema": "<gateway_storage_schema>",
  "gateway_definition": {
    "connection_name": "conn_restaurantops",
    "gateway_storage_catalog": "<catalog_name>",
    "gateway_storage_schema": "<gateway_storage_schema>",
    "gateway_storage_name": "gw_ingestion_silver"
  },
  "clusters": [
    {
      "label": "default",
      "policy_id": "<policy_id>",
      "apply_policy_default_values": true
    }
  ],
  "continuous": true
}'
```

### Result

After the pipeline is updated:

- Lakeflow Connect continues to create compute automatically.
- Every newly created cluster follows the specified Compute Policy.
- The policy enforces:
  - Fixed number of workers.
  - Fixed driver node type.
  - Fixed worker node type.
- This helps prevent Lakeflow Connect from provisioning larger clusters than intended.

---

## Way 2: Request an Azure Quota Increase

If your Azure subscription does not have sufficient vCPU quota, request a quota increase.

1. Open the **Azure Portal**.
2. Navigate to **Quotas**.
3. Select the appropriate **Subscription** and **Region**.
4. Search for the required **VM family** (for example, **Standard DSv4 Family vCPUs**).
5. Select the quota.
6. Click **Request New Quota**.
7. Specify the desired quota limit and submit the request.

> **Note:** Quota increases are generally available only for **Pay-As-You-Go** and other paid Azure subscriptions. Azure Free subscriptions typically cannot request quota increases.