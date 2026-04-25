# Web

Area router for `/web/`. Next.js 15.2.8 app deployed to Azure Static Web Apps at `schnapp.bet`. Shell is live; per-sport pages vary in maturity.

## Per-sport docs

- `/web/nba/README.md` - STATUS: live
- `/web/mlb/README.md` - STATUS: in development (3 of 6 ADR-0003 pages coded, not considered live)
- `/web/nfl/README.md` - STATUS: not started
- `/web/_shared/README.md` - shared shell and cross-sport components

## Files

Next.js app structure: `app/` for routes, `components/` for shared components, `app/api/` for API routes. Sport-specific pages live under `app/<sport>/`. Build config in `next.config.js` and `staticwebapp.config.json`.

## Key Concepts

Passcode-gated access via `common.user_codes`. Demo mode fixes the view to a historical date per `common.demo_config`. The connected visual pattern drives multi-visual updates from a single selected player (see `/docs/PRODUCT_BLUEPRINT.md`).

Site-wide maintenance gate in `middleware.ts` runs before the passcode layer. Toggle is two hardcoded constants at the top of the file: `MAINTENANCE_ON` (boolean) and `UNLOCK_CODE` (string). To lock the site, flip `MAINTENANCE_ON` to `true`, commit, push; SWA redeploys in ~90s. To unlock yourself, visit any URL with `?unlock=<UNLOCK_CODE>` once; the middleware sets the `sb_unlock` HttpOnly cookie for 30 days and 307-redirects to the clean URL. `/api/ping` is always allowed through so the DB keep-alive ping keeps working.

API routes talk to the VM's Flask service via the Cloudflare tunnel (`live.schnapp.bet`) or internal VM IP, depending on route.

## Invariants

- One Next.js app for all sports. No separate app per sport.
- Passcode check happens at the route layer before page content renders.
- Connected visual state lives at the page level, not inside individual components.

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[web]`.

## Open Questions

None at area level. Sport-specific questions live in the per-sport READMEs.
