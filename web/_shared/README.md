# Shared Web Components

**STATUS:** in development. NBA was built first, so many effectively shared components currently live under NBA-specific paths.

## Purpose

Shared components and utilities that apply across sports: layout shell, passcode gate, navigation, theme tokens, the connected-visual state pattern.

## Files

To be enumerated during Step 4 of the documentation restructure. Candidate shared components include layout wrappers, the passcode gate, top navigation, and any table or card primitives that are already used across sports.

## Key Concepts

The connected-visual pattern uses React context or lifted state at the page level so multiple visuals subscribe to a single selected-player state. See `/docs/PRODUCT_BLUEPRINT.md`.

The passcode gate wraps protected routes and checks `common.user_codes` before allowing access. Demo mode is applied at the same layer, pinning the viewed date per `common.demo_config`.

## Invariants

- Shared components never import from sport-specific folders.
- Sport-specific overrides flow in via props, not by shared code reaching into sport-specific modules.

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[shared][web]`.

## Open Questions

Which currently-NBA-located components should be promoted to `_shared` as MLB and NFL come online.
