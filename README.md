# SAP S/4HANA Skills Portal

Central hub for all SAP S/4HANA automated data generators and system administration tools.
Hosted on sapidesecc8 as a web portal linking to documentation, GitHub repos, and Claude Code skills for each business process.

## Live Portal

**URL:** `https://sapidesecc8.fivetran-internal-sales.com/sap_skills/`

## Server Access

| Property | Value |
|----------|-------|
| Hostname | `sapidesecc8.fivetran-internal-sales.com` |
| OS | SUSE Linux Enterprise Server 15 SP5 |
| SSH | `ssh root@sapidesecc8` (password: `(see vault)`) |
| Python | `/usr/sap/miniconda/bin/python3` (3.13.9) |
| Web port | 443 (HTTPS, production) / 8443 (HTTPS, dev) |

## Business Process Skills

| Skill | Process | GitHub | Documentation |
|-------|---------|--------|---------------|
| Order to Cash (OTC) | Sales & Distribution | [sap-otc-workflow](https://github.com/fivetran/sap-otc-workflow) | [HTML Doc](https://sapidesecc8.fivetran-internal-sales.com/sap_skills/docs/SAP_OTC_Workflow_Documentation.html) |
| Procure to Pay (P2P) | Materials Management | [sap-ptp-generator](https://github.com/fivetran/sap-ptp-generator) | [HTML Doc](https://sapidesecc8.fivetran-internal-sales.com/sap_skills/docs/SAP_P2P_Workflow_Documentation.html) |
| Plan to Produce (PP) | Production Planning | [PlanToProduce](https://github.com/fivetran/PlanToProduce) | [HTML Doc](https://sapidesecc8.fivetran-internal-sales.com/sap_skills/docs/SAP_PP_Workflow_Documentation.html) |
| Material Requirements Planning (MRP) | Supply Chain Planning | [MaterialRequirementsPlanning](https://github.com/fivetran/MaterialRequirementsPlanning) | [HTML Doc](https://sapidesecc8.fivetran-internal-sales.com/sap_skills/docs/SAP_MRP_Workflow_Documentation.html) |
| Housekeeping & Job Scheduler | System Administration | [SAPHousekeeping](https://github.com/fivetran/SAPHousekeeping) | [HTML Doc](https://sapidesecc8.fivetran-internal-sales.com/sap_skills/docs/SAP_Housekeeping_Documentation.html) |
| CDS View Extraction | Metadata & SQL Translation | [CDS-metadata-retrieval](https://github.com/fivetran/CDS-metadata-retrieval-fr-custom-SDK) | [HTML Doc](https://sapidesecc8.fivetran-internal-sales.com/sap_skills/docs/SAP_CDS_Workflow_Documentation.html) |
| GCS Parquet Explorer | Datalake Reader | [gcs-parquet-explorer](https://github.com/fivetran-antoniocarbone/gcs-parquet-explorer) | [HTML Doc](https://sapidesecc8.fivetran-internal-sales.com/sap_skills/docs/SAP_GCS_Explorer_Documentation.html) |
| Skills Portal | Server & Documentation | [SAPSkillsPortal](https://github.com/fivetran/SAPSkillsPortal) | [HTML Doc](https://sapidesecc8.fivetran-internal-sales.com/sap_skills/docs/SAP_Skills_Portal_Server_Documentation.html) |

## Architecture

The portal is served by the GCS Parquet Explorer web server on sapidesecc8 (HTTPS, port 443). Static files live in `/usr/sap/sap_skills/` on the server. The `/sap_skills/` route is handled by a static file handler in `gcs_explorer_server.py`.

All business process tools connect to SAP S/4HANA 2023 (sapidess4, system number 03, client 100) via SAP JCo 3 RFC from a local Mac. Direct HANA SQL access is available through an SSH tunnel to port 30015.

> **SAP Connection Details** (host, client, user, password, HANA access) are available from the SAP Specialists: **Antonio Carbone** and **Richard Brouwer**. See the [SAP Skills Portal](https://sapidesecc8.fivetran-internal-sales.com/sap_skills/) for the full setup guide.

## SSL Certificate

| Property | Value |
|----------|-------|
| Issuer | ZeroSSL |
| Subject | CN=sapidesecc8.fivetran-internal-sales.com |
| Expires | **Jun 25, 2026** |
| Cert file | `/usr/sap/gcs_explorer_cert.pem` |
| Key file | `/usr/sap/gcs_explorer_key.pem` |

## Resilience

| Service | Purpose | Auto-restart |
|---------|---------|--------------|
| `gcs-explorer.service` | Production (port 443) | `Restart=always`, `RestartSec=5` |
| `gcs-explorer-dev.service` | Dev (port 8443) | `Restart=always`, `RestartSec=5` |
| `gcs-explorer-watchdog.timer` | Health check every 60s | Restarts services on failure |

All services are `enabled` (auto-start on boot).

## Backup

| Property | Value |
|----------|-------|
| Schedule | **Daily at 23:00, Monday-Friday** |
| GCS bucket | `gs://sap-hana-backint/sapidesecc8_webserver/` |
| Script | `/usr/local/bin/backup_webserver.sh` |
| Log | `/var/log/webserver_backup.log` |

### What gets backed up

- Web server Python files (production + dev)
- SSL certificate and key
- Environment file (Polaris credentials)
- SAP Skills Portal (landing page, all docs, downloads)
- Systemd service units
- Restore guide

## Deploying Updates

```bash
# Upload landing page (no restart needed)
scp index.html root@sapidesecc8:/usr/sap/sap_skills/index.html

# Upload a doc
scp docs/SAP_Skills_Portal_Server_Documentation.html root@sapidesecc8:/usr/sap/sap_skills/docs/
```

## Color Themes

| Skill | Color | Hex |
|-------|-------|-----|
| OTC | Navy | `#1a3a5c` |
| P2P | Green | `#1a5c3a` |
| PP | Purple | `#4a1a5c` |
| MRP | Amber | `#b35900` |
| Housekeeping | Crimson | `#922b21` |
| CDS | Blue | `#0073FF` |
| GCS Explorer | Grey | `#6b7280` |
| Skills Portal | Gold | `#d4a017` |

## Contacts

For SAP system credentials, connection details, and technical support:
- **Antonio Carbone**
- **Richard Brouwer**
