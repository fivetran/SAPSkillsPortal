# SAP S/4HANA Skills Portal — Expert Context

You are an expert on the SAP S/4HANA Skills Portal, the central hub for all business process
automation tools. Use this context for any task involving the portal, deploying documentation,
managing the web server route, or coordinating across business process skills.

---

## Architecture Overview

### Portal URL

`https://sapidesecc8.fivetran-internal-sales.com/sap_skills/`

### Server

- **Host**: `sapidesecc8.fivetran-internal-sales.com`
- **Web server**: `gcs_explorer_server.py` (Python, HTTPS port 443)
- **Service**: `gcs-explorer.service` (systemd, `Restart=always`, `enabled`)
- **Static files**: `/usr/sap/sap_skills/` on sapidesecc8
- **Route**: `/sap_skills/` path handled by static file handler in `_do_GET()`
- **No auth required**: The `/sap_skills/` route is public (unlike `/datalake_reader/` which requires login)

### Directory Structure (sapidesecc8:/usr/sap/sap_skills/)

```
index.html                                          # Landing page
docs/
  SAP_OTC_Workflow_Documentation.html               # Order to Cash
  SAP_P2P_Workflow_Documentation.html               # Procure to Pay
  SAP_PP_Workflow_Documentation.html                # Plan to Produce
  SAP_MRP_Workflow_Documentation.html               # Material Requirements Planning
  SAP_Housekeeping_Documentation.html               # Housekeeping & Job Scheduler
  SAP_Skills_Portal_Documentation.html              # This portal's own documentation
```

### Local Project Directory

`/Users/antonio.carbone/SAP_Skills/`

---

## All Business Process Skills

### Order to Cash (OTC)

| Property | Value |
|----------|-------|
| Color | Navy `#1a3a5c` |
| GitHub | [fivetran/sap-otc-workflow](https://github.com/fivetran/sap-otc-workflow) |
| Doc URL | `/sap_skills/docs/SAP_OTC_Workflow_Documentation.html` |
| Local dir | `/Users/antonio.carbone/SAP_OTC/` |
| Skill | `/Users/antonio.carbone/.claude/commands/SAP_OTC/sap-otc.md` |
| Process | Customer master, sales order, delivery, PGI, billing, accounting |

### Procure to Pay (P2P)

| Property | Value |
|----------|-------|
| Color | Green `#1a5c3a` |
| GitHub | [fivetran/sap-ptp-generator](https://github.com/fivetran/sap-ptp-generator) |
| Doc URL | `/sap_skills/docs/SAP_P2P_Workflow_Documentation.html` |
| Local dir | `/Users/antonio.carbone/P2P/` |
| Skill | `/Users/antonio.carbone/.claude/commands/P2P/sap-p2p.md` |
| Process | Vendor master, purchase requisition, PO, goods receipt, invoice, payment |

### Plan to Produce (PP)

| Property | Value |
|----------|-------|
| Color | Purple `#4a1a5c` |
| GitHub | [fivetran/PlanToProduce](https://github.com/fivetran/PlanToProduce) |
| Doc URL | `/sap_skills/docs/SAP_PP_Workflow_Documentation.html` |
| Local dir | `/Users/antonio.carbone/PP/` |
| Skill | `/Users/antonio.carbone/.claude/commands/PP/sap-pp.md` |
| Process | BOM, routing, production order, goods issue, confirmation, goods receipt |

### Material Requirements Planning (MRP)

| Property | Value |
|----------|-------|
| Color | Amber `#b35900` |
| GitHub | [fivetran/MaterialRequirementsPlanning](https://github.com/fivetran/MaterialRequirementsPlanning) |
| Doc URL | `/sap_skills/docs/SAP_MRP_Workflow_Documentation.html` |
| Local dir | `/Users/antonio.carbone/MRP/` |
| Skill | `/Users/antonio.carbone/.claude/commands/MRP/sap-mrp.md` |
| Process | MRP run (MD01/MD02), planned orders, conversion to production/purchase orders |

### Housekeeping & Job Scheduler

| Property | Value |
|----------|-------|
| Color | Crimson `#922b21` |
| GitHub | [fivetran/SAPHousekeeping](https://github.com/fivetran/SAPHousekeeping) |
| Doc URL | `/sap_skills/docs/SAP_Housekeeping_Documentation.html` |
| Local dir | `/Users/antonio.carbone/SAP_ADMIN/` |
| Skill | `/Users/antonio.carbone/.claude/commands/SAP_ADMIN/sap-housekeeping.md` |
| Process | MM period opening, FI period monitoring, ML period extension, number range tracking |

### CDS View Extraction

| Property | Value |
|----------|-------|
| Color | Blue `#0073FF` |
| GitHub | [fivetran/CDS-metadata-retrieval-fr-custom-SDK](https://github.com/fivetran/CDS-metadata-retrieval-fr-custom-SDK) |
| Local dir | `/Users/antonio.carbone/cds_enhancement_20260210/github_release/src/` |
| Production | `sapidess4:/usr/sap/cds_sql_only/` |
| Process | 8-phase pipeline: dependency resolution, metadata, SQL extraction, HANA-to-ANSI translation |

---

## Deployment

### Updating the Landing Page

```bash
# Edit locally
vi /Users/antonio.carbone/SAP_Skills/index.html

# Upload to server (no restart needed)
scp /Users/antonio.carbone/SAP_Skills/index.html root@sapidesecc8:/usr/sap/sap_skills/index.html
```

### Adding a New Documentation Page

```bash
# Upload doc
scp /path/to/new_doc.html root@sapidesecc8:/usr/sap/sap_skills/docs/

# Update landing page to include link, then upload
```

### Updating an Existing Doc

```bash
# Re-upload the doc (overwrites)
scp /Users/antonio.carbone/P2P/docs/SAP_P2P_Workflow_Documentation.html \
    root@sapidesecc8:/usr/sap/sap_skills/docs/SAP_P2P_Workflow_Documentation.html
```

### Server Route (in gcs_explorer_server.py)

The `/sap_skills/` route is a static file handler injected into `_do_GET()` of the GCS Parquet Explorer server. It:
1. Redirects `/sap_skills` to `/sap_skills/`
2. Maps `/sap_skills/` to `/usr/sap/sap_skills/index.html`
3. Serves any file under `/usr/sap/sap_skills/` with correct MIME types
4. Blocks path traversal (`..` in path)
5. Requires no authentication (public)

If `gcs_explorer_server.py` is replaced, the route must be re-added. The block is inserted before `route = self._strip_base(parsed.path)` in `_do_GET()`.

---

## SAP System Details

| Parameter | Value |
|-----------|-------|
| System | SAP S/4HANA 2023 |
| Host | `sapidess4.fivetran-internal-sales.com` |
| System Number | `03` (gateway port 3303) |
| Client | `100` |
| User | `IDES` |
| Company Code | `1710` |
| Plant | `1710` |
| HANA DB | `FIV` on `sapidess4:30015` (via SSH tunnel) |

### Connectivity

- **RFC (JCo)**: Local Mac → sapidess4:3303 via SAP JCo 3 (`sapjco3.jar` + `libsapjco3.dylib`)
- **HANA SQL**: SSH tunnel `localhost:30015 → sapidess4:30015`, connect with hdbcli user `SAPHANADB`
- **Web portal**: sapidesecc8:443 (HTTPS, self-signed cert)

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Portal returns 404 | Check route exists in `gcs_explorer_server.py _do_GET()` |
| Server not running | `ssh root@sapidesecc8 "systemctl restart gcs-explorer"` |
| Doc not loading | Verify file exists: `ssh root@sapidesecc8 "ls /usr/sap/sap_skills/docs/"` |
| After server.py update | Re-add `/sap_skills/` route block to `_do_GET()` |
| SSL error | Cert at `/usr/sap/gcs_explorer_cert.pem` — self-signed, browser warning expected |
