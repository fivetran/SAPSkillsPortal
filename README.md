# SAP S/4HANA Skills Portal

Central hub for all SAP S/4HANA automated data generators and system administration tools.
Hosted on sapidesecc8 as a web portal linking to documentation, GitHub repos, and Claude Code skills for each business process.

## Live Portal

**URL:** `https://sapidesecc8.fivetran-internal-sales.com/sap_skills/`

## Business Process Skills

| Skill | Process | GitHub | Documentation |
|-------|---------|--------|---------------|
| Order to Cash (OTC) | Sales & Distribution | [sap-otc-workflow](https://github.com/fivetran/sap-otc-workflow) | [HTML Doc](https://sapidesecc8.fivetran-internal-sales.com/sap_skills/docs/SAP_OTC_Workflow_Documentation.html) |
| Procure to Pay (P2P) | Materials Management | [sap-ptp-generator](https://github.com/fivetran/sap-ptp-generator) | [HTML Doc](https://sapidesecc8.fivetran-internal-sales.com/sap_skills/docs/SAP_P2P_Workflow_Documentation.html) |
| Plan to Produce (PP) | Production Planning | [PlanToProduce](https://github.com/fivetran/PlanToProduce) | [HTML Doc](https://sapidesecc8.fivetran-internal-sales.com/sap_skills/docs/SAP_PP_Workflow_Documentation.html) |
| Material Requirements Planning (MRP) | Supply Chain Planning | [MaterialRequirementsPlanning](https://github.com/fivetran/MaterialRequirementsPlanning) | [HTML Doc](https://sapidesecc8.fivetran-internal-sales.com/sap_skills/docs/SAP_MRP_Workflow_Documentation.html) |
| Housekeeping & Job Scheduler | System Administration | [SAPHousekeeping](https://github.com/fivetran/SAPHousekeeping) | [HTML Doc](https://sapidesecc8.fivetran-internal-sales.com/sap_skills/docs/SAP_Housekeeping_Documentation.html) |
| CDS View Extraction | Metadata & SQL Translation | [CDS-metadata-retrieval](https://github.com/fivetran/CDS-metadata-retrieval-fr-custom-SDK) | — |

## Related Tools

| Tool | URL | GitHub |
|------|-----|--------|
| GCS Parquet Explorer | [datalake_reader](https://sapidesecc8.fivetran-internal-sales.com/datalake_reader/) | [gcs-parquet-explorer](https://github.com/fivetran-antoniocarbone/gcs-parquet-explorer) |
| SAP Skills Portal | [sap_skills](https://sapidesecc8.fivetran-internal-sales.com/sap_skills/) | [SAPSkillsPortal](https://github.com/fivetran/SAPSkillsPortal) |

## Architecture

All tools connect to SAP S/4HANA 2023 (sapidesecc8, system number 03, client 100) via SAP JCo 3 RFC from a local Mac. Direct HANA SQL access is available through an SSH tunnel to port 30015 using hdbcli (Python). Each skill is a standalone Java program with a Claude Code skill file for AI-assisted operation.

## Hosting

The portal is served by the GCS Parquet Explorer web server on sapidesecc8 (HTTPS, port 443). Static files live in `/usr/sap/sap_skills/` on the server. The `/sap_skills/` route is handled by a static file handler added to `gcs_explorer_server.py`.

### Deploying Updates

```bash
# Upload landing page
scp index.html root@sapidesecc8:/usr/sap/sap_skills/index.html

# Upload a doc
scp docs/SAP_Skills_Portal_Documentation.html root@sapidesecc8:/usr/sap/sap_skills/docs/

# No server restart needed — files are served from disk
```

## Color Themes

Each skill has a unique color accent:

| Skill | Color | Hex |
|-------|-------|-----|
| OTC | Navy | `#1a3a5c` |
| P2P | Green | `#1a5c3a` |
| PP | Purple | `#4a1a5c` |
| MRP | Amber | `#b35900` |
| Housekeeping | Crimson | `#922b21` |
| CDS / Portal | Blue | `#0073FF` |
