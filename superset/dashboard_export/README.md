# Dashboard Export

This directory is reserved for Superset dashboard exports.

The Phase 12 deliverable documents the dashboard specification in `docs/superset_dashboard.md`. A real export should be generated from a running Superset instance after:

1. ClickHouse has loaded Silver data.
2. dbt has built the marts.
3. Superset has connected to ClickHouse.
4. The `NYC Taxi Operations` dashboard has been created.

Do not commit exports that contain real credentials.
