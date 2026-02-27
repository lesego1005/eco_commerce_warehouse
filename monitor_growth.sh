#!/bin/bash
export PGPASSFILE='/c/Users/Dell/pgpass.conf'

# Run the snapshot query
"/c/Program Files/PostgreSQL/18/bin/psql.exe" -U postgres -d eco_warehouse -c "
INSERT INTO db_table_sizes_snapshot(relname, total_bytes, table_bytes)
SELECT relname, pg_total_relation_size(relid), pg_relation_size(relid)
FROM pg_catalog.pg_statio_user_tables;"

echo "Snapshot captured at $(date)"