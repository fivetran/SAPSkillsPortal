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
| Python | `/root/miniconda/bin/python3` (3.13.9) |
| Web port | 443 (HTTPS, production) / 8443 (HTTPS, dev) |

### How to SSH

```bash
# Direct SSH (password: see vault)
ssh root@sapidesecc8

# Or with full hostname
ssh root@sapidesecc8.fivetran-internal-sales.com
```

Accept the host key on first connection. Password is `(see vault)`.

---

## Architecture

### Web Server

- **Process**: `gcs_explorer_server.py` (Python HTTPS server on port 443)
- **Service**: `gcs-explorer.service` (systemd, `Restart=always`, `enabled`)
- **Static files**: `/usr/sap/sap_skills/` on sapidesecc8
- **Route**: `/sap_skills/` path handled by static file handler in `_do_GET()`
- **No auth required**: The `/sap_skills/` route is public

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

## Important Notes

- **Python path**: Always use `/root/miniconda/bin/python3`, NOT `/usr/bin/python3`
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
