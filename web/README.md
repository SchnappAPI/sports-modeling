# Web

Area router for `/web/`. Next.js 15.2.8 app deployed to Azure Static Web Apps at `schnapp.bet`. Shell is live; per-sport pages vary in maturity.

## Per-sport docs

- `/web/nba/README.md` - STATUS: live
- `/web/mlb/README.md` - STATUS: design phase
- `/web/nfl/README.md` - STATUS: planning
- `/web/_shared/README.md` - shared shell and cross-sport components

## Files

Next.js app structure: `app/` for routes, `components/` for shared components, `app/api/` for API routes. Sport-specific pages live under `app/<sport>/`. Build config in `next.config.js` and `staticwebapp.config.json`.

## Key Concepts

Passcode-gated access via `common.user_codes`. Demo mode fixes the view to a historical date per `common.demo_config`. The connected visual pattern drives multi-visual updates from a single selected player (see `/docs/PRODUCT_BLUEPRINT.md`).

API routes talk to the VM's Flask service via the Cloudflare tunnel (`live.schnapp.bet`) or internal VM IP, depending on route.

## Invariants

- One Next.js app for all sports. No separate app per sport.
- Passcode check happens at the route layer before page content renders.
- Connected visual state lives at the page level, not inside individual components.

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[web]`.

## Open Questions

None at area level. Sport-specific questions live in the per-sport READMEs.
