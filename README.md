# sports-modeling

Monorepo for schnapp.bet — a sports prop research platform covering NBA, MLB, and NFL.

Personal project. 

## Stack

Next.js 15 on Azure Static Web Apps. Azure SQL Serverless. Python ETL via GitHub Actions on a self-hosted Azure VM runner. Flask live-data service and a FastMCP server on the same VM, exposed through Cloudflare Tunnel.

## Where to start

- `/docs/README.md` — documentation router, reading order, tag taxonomy.
- `/CLAUDE.md` — context for Claude sessions in this repo.
- `/docs/CONNECTIONS.md` — single source of truth for every external system and credential.
- `/docs/SESSION_PROTOCOL.md` — session start/end protocol.
