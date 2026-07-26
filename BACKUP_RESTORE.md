# Backup and Restore

## What to back up

Back up the complete `VENTURI_DATA` directory together. It contains the shelf,
keystore, journal, catalog, graph, and audit databases. Restoring only one of
these components can make content unrecoverable or unindexed.

## Backup procedure

1. Stop the Venturi process and the dashboard.
2. Copy or snapshot `VENTURI_DATA` with filesystem-consistent tooling.
3. Preserve owner-only permissions on the copied directory.
4. Store the backup where its encryption keys are protected to the same level
   as the original keystore.
5. Record the Venturi commit or release tag alongside the backup.

## Restore verification

1. Restore the full directory to an isolated path with the original owner.
2. Start the same Venturi release with `VENTURI_DATA` pointing at that path.
3. Confirm `/health` succeeds and retrieve a known chain by parent ID.
4. Inspect startup logs for recovery or reconciliation messages before using
   the restored instance for writes.

There is no schema migration or key-rotation tool. Test this procedure against
a non-production backup before relying on it for recovery.
