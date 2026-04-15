# SAP S/4HANA Skills Portal — Expert Context

You are an expert on the SAP S/4HANA Skills Portal, the central hub for all business process
automation tools. Use this context for any task involving the portal, deploying documentation,
managing the web server, backups, SSL certificates, or coordinating across business process skills.

---

## Portal URL

`https://sapidesecc8.fivetran-internal-sales.com/sap_skills/`

---

## Server Access

| Property | Value |
|----------|-------|
| Hostname | `sapidesecc8.fivetran-internal-sales.com` |
| OS | SUSE Linux Enterprise Server 15 SP5 |
| SSH | `ssh root@sapidesecc8` |
| SSH Password | `(see vault)` |
| Python | `/usr/sap/miniconda/bin/python3` (3.13.9) |
| Web port | 443 (HTTPS, production) / 8443 (HTTPS, dev) |

### How to SSH

```bash
# Direct SSH (password: see vault)
ssh root@sapidesecc8

# Or with full hostname
ssh root@sapidesecc8.fivetran-internal-sales.com
```

Accept the host key on first connection. Password is stored in the encrypted vault.

---

## Architecture

### Web Server

- **Process**: `gcs_explorer_server.py` (Python HTTPS server on port 443)
- **Service**: `gcs-explorer.service` (systemd, `Restart=always`, `enabled`)
- **Static files**: `/usr/sap/sap_skills/` on sapidesecc8
- **Route**: `/sap_skills/` path handled by static file handler in `_do_GET()`
- **No auth required**: The `/sap_skills/` route is public
- **Vault API**: POST `/sap_skills/api/vault/{action}` — CRUD for encrypted credentials (requires master password)
- **Admin API**: POST `/sap_skills/api/admin/{action}` — server operations:
  - `restart_production` — restarts gcs-explorer service (requires master password)
  - `view_logs` — returns last 40 lines of `/var/log/gcs_explorer.log` (no auth)
  - `disk_space` — returns `df -h` output (no auth)
  - `cron_jobs` — returns `crontab -l` output (no auth)
- **S/4HANA Cockpit API** (target: sapidess4 via SSH, GET unless noted):
  - `/sap_skills/api/system_status` — SAP + HANA FIV + PIT status, tenant names, last backup dates
  - `/sap_skills/api/hana_version` — HANA version, OS version, tenant name
  - `/sap_skills/api/hardware_info` — CPU count (nproc) and total memory (free -g) from sapidess4
  - `/sap_skills/api/backup_running` — check if a HANA backup is currently in progress
  - `/sap_skills/api/hana_ide_credentials` — Web IDE user/password from vault
  - `/sap_skills/api/disk_space` — filesystem usage parsed from df -h (excludes tmpfs/devtmpfs)
  - `/sap_skills/api/trigger_backup` — POST, starts BACKINT backup (target: fiv or pit)
  - `/sap_skills/api/hana_control` — POST, start/stop HANA (stop requires master password)
  - `/sap_skills/api/sap_control` — POST, start/stop SAP app server (stop requires master password)
  - `/sap_skills/api/s4h_license` — SAP license validity (NetWeaver + Maintenance) from SAPLIKEY
- **ECC Oracle Cockpit API** (target: sapidesecc8 local, GET unless noted):
  - `/sap_skills/api/ecc_system_status` — SAP + Oracle status
  - `/sap_skills/api/ecc_disk_space` — filesystem usage
  - `/sap_skills/api/ecc_sap_control` — POST, start/stop SAP (stop requires master password)
  - `/sap_skills/api/ecc_oracle_control` — POST, start/stop Oracle (stop requires master password)
  - `/sap_skills/api/ecc_trigger_backup` — POST, Oracle offline backup via brbackup
  - `/sap_skills/api/ecc_license` — SAP license validity from SAPLIKEY via sqlplus (oraaba user)
- **ECC SQL Server Cockpit API** (target: sap-sql-ides/10.128.0.51 via WinRM, GET unless noted):
  - `/sap_skills/api/sq1_system_status` — SAP status (SOAP) + SQL Server status (helper: sq1_db_query.py)
  - `/sap_skills/api/sq1_disk_space` — Windows disk space via WinRM Get-WmiObject
  - `/sap_skills/api/sq1_sap_control` — POST, start/stop SAP via SOAP (D00 + ASCS01, stop needs master pwd)
  - `/sap_skills/api/sq1_trigger_backup` — POST, SQL Server full backup to D:\Backup (helper: sq1_trigger_backup.py)
  - `/sap_skills/api/sq1_license` — SAP license validity from SAPLIKEY via WinRM sqlcmd (helper: sq1_license_query.py)
  - `/sap_skills/api/sq1_credential` — sq1adm password from vault

### Directory Structure (sapidesecc8:/usr/sap/sap_skills/)

```
index.html                                          # Landing page
docs/
  SAP_OTC_Workflow_Documentation.html               # Order to Cash
  SAP_P2P_Workflow_Documentation.html               # Procure to Pay
  SAP_PP_Workflow_Documentation.html                # Plan to Produce
  SAP_MRP_Workflow_Documentation.html               # Material Requirements Planning
  SAP_Housekeeping_Documentation.html               # Housekeeping & Job Scheduler
  SAP_CDS_Workflow_Documentation.html               # CDS View Extraction
  SAP_GCS_Explorer_Documentation.html               # GCS Parquet Explorer
  SAP_Skills_Portal_Documentation.html              # Portal's own documentation
  SAP_Management_Cockpit.html                       # Management Cockpit hub (3 systems)
  SAP_S4HANA_2023.html                              # S/4HANA 2023 Management Cockpit
  SAP_S4HANA_2023_Guide.html                        # S/4HANA 2023 Cockpit User Guide
  SAP_ECC6_EHP8.html                                # ECC 6.0 EHP8 Management Cockpit (Oracle)
  SAP_ECC6_EHP8_Guide.html                          # ECC 6.0 EHP8 Cockpit User Guide
  SAP_ECC6_SQ1.html                                 # ECC 6.0 SQ1 Management Cockpit (SQL Server)
downloads/
  sapjco3.jar                                       # SAP JCo library
  libsapjco3.dylib                                  # JCo native library (macOS)
  gson.jar                                          # Google Gson
```

### Local Project Directory

`/Users/antonio.carbone/SAP_Skills/`

---

## SSL Certificate

| Property | Value |
|----------|-------|
| Issuer | ZeroSSL RSA Domain Secure Site CA |
| Subject | CN=sapidesecc8.fivetran-internal-sales.com |
| Expires | **Jun 25, 2026** |
| Cert file | `/usr/sap/gcs_explorer_cert.pem` (chained: server + intermediate CA) |
| Key file | `/usr/sap/gcs_explorer_key.pem` |

### Check certificate expiry

```bash
ssh root@sapidesecc8 "openssl x509 -in /usr/sap/gcs_explorer_cert.pem -noout -dates"
```

### Renew certificate

1. Go to https://zerossl.com and renew for `sapidesecc8.fivetran-internal-sales.com`
2. Download the new certificate and key
3. Upload and restart:

```bash
scp new_cert.pem root@sapidesecc8:/usr/sap/gcs_explorer_cert.pem
scp new_key.pem root@sapidesecc8:/usr/sap/gcs_explorer_key.pem
ssh root@sapidesecc8 "chmod 600 /usr/sap/gcs_explorer_key.pem && systemctl restart gcs-explorer && systemctl restart gcs-explorer-dev"
```

**Important:** The cert PEM must be chained (server cert + intermediate CA concatenated):
```bash
cat server.crt intermediate.crt > /usr/sap/gcs_explorer_cert.pem
```

---

## Resilience & Systemd Services

### Services

| Service | Purpose | Port |
|---------|---------|------|
| `gcs-explorer.service` | Production web server | 443 |
| `gcs-explorer-dev.service` | Dev web server | 8443 |
| `gcs-explorer-watchdog.timer` | Health check every 60s | — |

All services have `Restart=always` with `RestartSec=5` and are `enabled` (auto-start on boot).

### Check service status

```bash
ssh root@sapidesecc8 "systemctl status gcs-explorer --no-pager"
ssh root@sapidesecc8 "systemctl status gcs-explorer-dev --no-pager"
ssh root@sapidesecc8 "systemctl status gcs-explorer-watchdog.timer --no-pager"
```

### Restart services

```bash
ssh root@sapidesecc8 "systemctl restart gcs-explorer"
ssh root@sapidesecc8 "systemctl restart gcs-explorer-dev"
```

### Health watchdog

The watchdog runs every 60 seconds (first run 120s after boot). It:
1. Sends a health check to `https://localhost/datalake_reader/api/health`
2. If no response within 10 seconds, restarts the production service
3. Does the same for the dev service on port 8443
4. Logs restarts to `/var/log/gcs_explorer_watchdog.log`

---

## Backup

### GCS Backup Location

`gs://sap-hana-backint/sapidesecc8_webserver/`

### Scheduled Backup

| Property | Value |
|----------|-------|
| Schedule | **Daily at 23:00, Monday-Friday** (weekends excluded) |
| Script | `/usr/local/bin/backup_webserver.sh` on sapidesecc8 |
| Log | `/var/log/webserver_backup.log` |
| Mechanism | cron |

### What's backed up

- `gcs_explorer_server.py` (production) and `gcs_explorer_server_dev.py` (dev)
- SSL certificate and key (`gcs_explorer_cert.pem`, `gcs_explorer_key.pem`)
- Environment file (`gcs_explorer.env` — Polaris OAuth credentials)
- SAP Skills Portal (`sap_skills/` — landing page, all docs, downloads)
- Systemd service units (production, dev, watchdog)
- Restore guide (`RESTORE_GUIDE.md`)

### Manual backup

```bash
ssh root@sapidesecc8 "/usr/local/bin/backup_webserver.sh"
ssh root@sapidesecc8 "cat /var/log/webserver_backup.log | tail -5"
```

### Check backup

```bash
gsutil ls -r gs://sap-hana-backint/sapidesecc8_webserver/
gsutil du -s gs://sap-hana-backint/sapidesecc8_webserver/
```

---

## Restore Procedure

### Step 1: Download backup from GCS

```bash
ssh root@sapidesecc8
mkdir -p /tmp/restore
gsutil -m rsync -r gs://sap-hana-backint/sapidesecc8_webserver/ /tmp/restore/
```

### Step 2: Restore web server files

```bash
cp /tmp/restore/gcs_explorer_server.py /usr/sap/gcs_explorer_server.py
cp /tmp/restore/gcs_explorer_server_dev.py /usr/sap/gcs_explorer_server_dev.py
cp /tmp/restore/gcs_explorer_cert.pem /usr/sap/gcs_explorer_cert.pem
cp /tmp/restore/gcs_explorer_key.pem /usr/sap/gcs_explorer_key.pem
chmod 600 /usr/sap/gcs_explorer_key.pem
cp /tmp/restore/gcs_explorer.env /usr/sap/gcs_explorer.env
chmod 600 /usr/sap/gcs_explorer.env
```

### Step 3: Restore SAP Skills Portal

```bash
mkdir -p /usr/sap/sap_skills/docs /usr/sap/sap_skills/downloads
cp /tmp/restore/sap_skills/index.html /usr/sap/sap_skills/
cp /tmp/restore/sap_skills/docs/*.html /usr/sap/sap_skills/docs/
cp /tmp/restore/sap_skills/downloads/* /usr/sap/sap_skills/downloads/
```

### Step 4: Install systemd services

```bash
cp /tmp/restore/systemd/gcs-explorer.service /etc/systemd/system/
cp /tmp/restore/systemd/gcs-explorer-dev.service /etc/systemd/system/
cp /tmp/restore/systemd/gcs-explorer-watchdog.service /etc/systemd/system/

# Recreate watchdog timer (not in backup)
cat > /etc/systemd/system/gcs-explorer-watchdog.timer << 'EOF'
[Unit]
Description=Run GCS Explorer Health Watchdog every 60s

[Timer]
OnBootSec=120
OnUnitActiveSec=60
AccuracySec=5

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable gcs-explorer gcs-explorer-dev gcs-explorer-watchdog.timer
systemctl start gcs-explorer gcs-explorer-dev gcs-explorer-watchdog.timer
```

### Step 5: Verify

```bash
systemctl status gcs-explorer --no-pager
curl -sk https://localhost/datalake_reader/api/health
curl -sk -o /dev/null -w "%{http_code}" https://localhost/sap_skills/
```

---

## S/4HANA 2023 Management Cockpit

### URL

`https://sapidesecc8.fivetran-internal-sales.com/sap_skills/docs/SAP_S4HANA_2023.html`

### Pages

| File | Purpose |
|------|---------|
| `SAP_S4HANA_2023.html` | Live cockpit — status, backup, start/stop, disk space, links |
| `SAP_S4HANA_2023_Guide.html` | User guide — how to use the cockpit |

### Target Server

| Property | Value |
|----------|-------|
| Hostname | `sapidess4.fivetran-internal-sales.com` |
| SID | `S4H` |
| Instance Nr | `03` |
| Client | `100` |

### HANA Instances

| Instance | SID | Nr | Port | OS User | Vault Key | Purpose |
|----------|-----|----|------|---------|-----------|---------|
| FIV | FIV | 00 | 30015 | fivadm | `sapidess4_hana` (tenant FIV, user SAPHANADB) | Primary S/4HANA database |
| PIT | PIT | 96 | 39613 | pitadm | `sapidess4_hana` (tenant PIT, user SYSTEM) | HANA Cockpit / DBA database |

### Cockpit Features

- **System Info**: Server details + disk space (side-by-side cards). CPUs and Memory loaded live.
- **Status**: Three cards (SAP App Server, HANA FIV, HANA PIT) with green/red dots, tenant names, last backup dates.
- **Backup**: On-demand BACKINT backup per HANA instance with live timer and refresh.
- **Start/Stop**: Start/stop SAP (`startsap R3`/`stopsap R3` as `s4hadm`) and HANA (`HDB start`/`HDB stop` as `fivadm`/`pitadm`). All stop operations require master password. Pre-checks prevent redundant start/stop.
- **Links**: SSO, HANA Cockpit, Web IDE (credentials auto-loaded from vault), GCP console, Backint bucket, Slab, all business process docs.
- **Credentials**: All from vault, never hardcoded. Reads don't need master password; stops do.

### Shutdown/Startup Order

- **Shutdown**: Stop SAP first, then HANA FIV. PIT is independent.
- **Startup**: Start HANA FIV first, then SAP. PIT is independent.

---

## ECC 6.0 EHP8 Management Cockpit (Oracle)

### URL

`https://sapidesecc8.fivetran-internal-sales.com/sap_skills/docs/SAP_ECC6_EHP8.html`

### Pages

| File | Purpose |
|------|---------|
| `SAP_ECC6_EHP8.html` | Live cockpit — status, backup, start/stop, disk space, license, links |
| `SAP_ECC6_EHP8_Guide.html` | User guide — how to use the cockpit |

### Target Server

| Property | Value |
|----------|-------|
| Hostname | `sapidesecc8.fivetran-internal-sales.com` |
| SID | `ABA` |
| Instance Nr | `00` |
| Client | `800` |
| Database | Oracle 19c |
| SAP Admin User | `abaadm` |
| Oracle User | `oraaba` (use for sqlplus, NOT `oracle` user) |

### Cockpit Features

- **System Info**: Server details + disk space with visual bars.
- **Status**: SAP Application Server + Oracle Database cards with green/red dots.
- **Backup**: On-demand Oracle offline backup via brbackup to `/media/oraclebackup` with GCS sync.
- **Start/Stop**: Start/stop SAP and Oracle independently. Stop requires master password.
- **License**: SAP license validity (NetWeaver + Maintenance) queried from SAPSR3.SAPLIKEY via sqlplus as oraaba.
- **Auto-refresh**: Status and license refreshed every 5 minutes.

### Helper Scripts (on sapidesecc8)

| Script | Purpose |
|--------|---------|
| `/usr/local/bin/ecc8_license_query.py` | Query SAPLIKEY via sqlplus as oraaba, returns JSON |

---

## ECC 6.0 SQ1 Management Cockpit (SQL Server)

### URL

`https://sapidesecc8.fivetran-internal-sales.com/sap_skills/docs/SAP_ECC6_SQ1.html`

### Target Server

| Property | Value |
|----------|-------|
| Hostname | `sap-sql-ides` |
| Internal IP | `10.128.0.51` |
| Public IP | `104.154.109.8` |
| SID | `SQ1` |
| Instance Nr | `00` (D00) + `01` (ASCS01) |
| Client | `800` |
| Database | Microsoft SQL Server 2012 SP1 (11.0.3393.0) — `SAP-SQL-IDES\SQ1` |
| OS | Windows Server 2022 Datacenter |
| SAP Admin User | `sq1adm` |
| Vault Key | `sapidessq1_os` |

### Remote Access Method

WinRM (pywinrm, port 5985, NTLM auth) — no SSH on Windows. SAP process control via sapcontrol SOAP (ports 50013 for D00, 50113 for ASCS01).

### Cockpit Features

- **System Info**: Server details, RDP button, disk space with visual bars via WinRM.
- **Status**: SAP Application Server (SOAP GetProcessList) + SQL Server Database (state, last backup with dual timezone, backup running indicator with percent complete).
- **Backup**: On-demand SQL Server full compressed backup to `D:\Backup`. Backup status refreshed every 5 minutes.
- **Start/Stop**: Start/stop SAP via authenticated SOAP to both D00 and ASCS01. Stop requires master password. Poll status after start/stop with 30s countdown.
- **License**: SAP license validity from SAPLIKEY via WinRM sqlcmd.
- **Password modal**: All operations requiring master password use a masked HTML modal (not native prompt).
- **Auto-refresh**: Status and license refreshed every 5 minutes.

### Helper Scripts (on sapidesecc8)

| Script | Purpose |
|--------|---------|
| `/usr/local/bin/sq1_db_query.py` | Query SQL Server state + last backup + backup running via WinRM |
| `/usr/local/bin/sq1_trigger_backup.py` | Trigger full compressed backup via WinRM PowerShell |
| `/usr/local/bin/sq1_license_query.py` | Query SAPLIKEY for license validity via WinRM sqlcmd |

### GCP VM Settings

`https://console.cloud.google.com/compute/instancesDetail/zones/us-central1-a/instances/sap-sql?project=internal-sales`

No DNS entry — use hostname `sap-sql-ides` or IP `104.154.109.8`.

---

## License Monitoring

All three cockpit pages include a **SAP License** card that:
- Queries the `SAPLIKEY` table for NetWeaver and Maintenance license entries
- Parses validity dates from the `0008YYYYMMDD` pattern in the VALUE field
- Displays status: Valid (green), Expiring Soon (orange, <30 days), Expired (red)
- Shows "Permanent" for licenses with end date 9999-12-31
- Auto-refreshes every 5 minutes

### API Endpoints

| System | Endpoint | Helper Script | Query Method |
|--------|----------|---------------|--------------|
| S/4HANA | `/sap_skills/api/s4h_license` | `s4h_license_query.py` | SSH → hdbsql (SAPHANADB@FIV:30015) |
| ECC Oracle | `/sap_skills/api/ecc_license` | `ecc8_license_query.py` | sqlplus as oraaba (local) |
| ECC SQL Server | `/sap_skills/api/sq1_license` | `sq1_license_query.py` | WinRM → sqlcmd |

---

## Deployment

### Update the landing page

```bash
scp /Users/antonio.carbone/SAP_Skills/index.html root@sapidesecc8:/usr/sap/sap_skills/index.html
```

No server restart needed — files are served directly from disk.

### Add/update a documentation page

```bash
scp /Users/antonio.carbone/SAP_Skills/docs/NEW_DOC.html root@sapidesecc8:/usr/sap/sap_skills/docs/
```

### Sync the documentation copy

```bash
cp /Users/antonio.carbone/SAP_Skills/index.html /Users/antonio.carbone/SAP_Skills/docs/SAP_Skills_Portal_Documentation.html
```

---

## All Business Process Skills

| Skill | Color | GitHub | Local Dir |
|-------|-------|--------|-----------|
| Order to Cash (OTC) | `#1a3a5c` | [fivetran/sap-otc-workflow](https://github.com/fivetran/sap-otc-workflow) | `~/SAP_OTC/` |
| Procure to Pay (P2P) | `#1a5c3a` | [fivetran/sap-ptp-generator](https://github.com/fivetran/sap-ptp-generator) | `~/P2P/` |
| Plan to Produce (PP) | `#4a1a5c` | [fivetran/PlanToProduce](https://github.com/fivetran/PlanToProduce) | `~/PP/` |
| MRP | `#b35900` | [fivetran/MaterialRequirementsPlanning](https://github.com/fivetran/MaterialRequirementsPlanning) | `~/MRP/` |
| Housekeeping | `#922b21` | [fivetran/SAPHousekeeping](https://github.com/fivetran/SAPHousekeeping) | `~/SAP_ADMIN/` |
| CDS View Extraction | `#0073FF` | [fivetran/CDS-metadata-retrieval-fr-custom-SDK](https://github.com/fivetran/CDS-metadata-retrieval-fr-custom-SDK) | `~/cds_enhancement_20260210/` |
| GCS Parquet Explorer | `#6b7280` | [fivetran-antoniocarbone/gcs-parquet-explorer](https://github.com/fivetran-antoniocarbone/gcs-parquet-explorer) | `~/Downloads/gcs_parquet_explorer/` |
| Skills Portal | `#d4a017` | [fivetran/SAPSkillsPortal](https://github.com/fivetran/SAPSkillsPortal) | `~/SAP_Skills/` |

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Portal returns 404 | Check `/sap_skills/` route exists in `gcs_explorer_server.py _do_GET()` |
| Server not running | `ssh root@sapidesecc8 "systemctl restart gcs-explorer"` |
| Doc page not loading | `ssh root@sapidesecc8 "ls /usr/sap/sap_skills/docs/"` |
| After server.py update | Re-add `/sap_skills/` route block to `_do_GET()` |
| SSL cert expired | Renew at zerossl.com, replace files, restart services |
| Backup not running | `ssh root@sapidesecc8 "crontab -l"` — verify cron entry exists |
| Watchdog not restarting | `systemctl status gcs-explorer-watchdog.timer --no-pager` |
| Changes not visible | Files served from disk — no cache, hard-refresh browser |

---

## Icon Creation for Link Resources

When adding links to portal pages (e.g., S/4HANA cockpit, Management Cockpit), create SVG icons
as simple, clean files saved to `/usr/sap/sap_skills/docs/` (and locally to `~/SAP_Skills/docs/`).

### SVG Icon Template

```svg
<svg xmlns="http://www.w3.org/2000/svg" width="200" height="200" viewBox="0 0 200 200">
  <rect width="200" height="200" rx="20" fill="#COLOR"/>
  <text x="100" y="120" text-anchor="middle" font-family="Arial,Helvetica,sans-serif"
        font-size="72" font-weight="bold" fill="white">TEXT</text>
</svg>
```

### Existing Icons

| File | Color | Text | Used For |
|------|-------|------|----------|
| `sap_icon.svg` | `#0070F2` (blue) | SAP | SSO S/4HANA (Okta SAML) |
| `hana_icon.svg` | `#c0392b` (red) | HANA | HANA Cockpit |
| `gcp_icon.svg` | `#4285F4` (blue) | GCP | GCP VM Settings, DNS Registration |
| `slab_icon.svg` | multi-color quadrants | — | Slab documentation links |

### Guidelines

- Use SVG format (not JPEG/PNG) — renders perfectly at any size, no conversion needed
- Save to `docs/` alongside the HTML pages
- Use `width="42" height="42"` and `border-radius:8px` in `<img>` tags
- Wrap icons in `<a>` tags to make them clickable (same href as the text link)
- For complex logos (like Slab), draw shapes with `<rect>` and `<line>` elements
- Font size: `72` for 3-char text (SAP, GCP), `58` for 4-char (HANA), adjust for longer text

---

## Important Notes

- **Python path**: Always use `/usr/sap/miniconda/bin/python3`, NOT `/usr/bin/python3`
- **SAP Skills Portal**: Static files only — no server restart needed after updating HTML/docs
- **Server.py updates**: After replacing `gcs_explorer_server.py`, run `systemctl restart gcs-explorer`
- **The `/sap_skills/` route** is inside `gcs_explorer_server.py` in `_do_GET()`. If server.py is replaced with a version missing this route, the portal returns 404.
- **Backup refresh**: After portal updates, either wait for the 23:00 cron or run manually: `ssh root@sapidesecc8 "/usr/local/bin/backup_webserver.sh"`
- **SSL cert expires Jun 25, 2026** — renew before this date

---

## Contacts

For SAP system credentials, connection details, and technical support:
- **Antonio Carbone**
- **Richard Brouwer**
