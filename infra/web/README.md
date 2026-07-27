# Web MVP Shell

This directory contains the initial frontend runtime for external service-only mode.

## Purpose

- Replaces the previous placeholder web container with a real static page served by `nginx:alpine`.
- Provides quick links and endpoint examples for validating API and Martin wiring.
- Acts as a thin transitional shell while product and map UX are still evolving.

## Runtime

The Docker compose `web` service mounts `infra/web/index.html` read-only into the Nginx default site and exposes it at `http://localhost:3001`.

## Migration Path

- Keep this shell minimal and operational for infra smoke checks.
- Build the full application in `apps/web` (planned Next.js app).
- When the full app is ready, replace the static mount with a dedicated app image/service while preserving the same host port mapping contract.

## Security

No secrets are required in this folder. Keep credentials in local env files such as `infra/docker/stack.external.env`.
