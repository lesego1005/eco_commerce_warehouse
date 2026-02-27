#!/bin/bash
# Configuration
DB_NAME="eco_warehouse"
DB_USER="postgres"
REPORT_FILE="db_health_report_$(date +%Y%m%d).md"
PG_BIN="/c/Program Files/PostgreSQL/18/bin"

echo "# Database Health Report: $DB_NAME" > $REPORT_FILE
echo "Generated at: $(date)" >> $REPORT_FILE
echo "---" >> $REPORT_FILE

# 1. Check Table Growth (Alert if > 100MB increase)
echo "## 📈 Table Growth (Top 5)" >> $REPORT_FILE
"$PG_BIN/psql.exe" -U $DB_USER -d $DB_NAME -c "
SELECT relname, 
       pg_size_pretty(total_bytes) as size,
       CASE WHEN total_bytes > 104857600 THEN '⚠️ LARGE' ELSE '✅ OK' END as status
FROM db_table_sizes_snapshot 
WHERE snapshot_ts >= now() - interval '24 hours'
ORDER BY total_bytes DESC LIMIT 5;" >> $REPORT_FILE

# 2. Check for Slow Queries
echo "## 🐢 Slowest Queries (Mean Time)" >> $REPORT_FILE
"$PG_BIN/psql.exe" -U $DB_USER -d $DB_NAME -c "
SELECT substring(query, 1, 50) as query_part, 
       calls, 
       round(mean_exec_time::numeric, 2) as avg_ms
FROM pg_stat_statements 
ORDER BY mean_exec_time DESC LIMIT 5;" >> $REPORT_FILE

# 3. Check Connection Limits
echo "## 🔌 Connection Usage" >> $REPORT_FILE
"$PG_BIN/psql.exe" -U $DB_USER -d $DB_NAME -c "
SELECT count(*) as active_conns,
       setting::int as max_conns,
       round((count(*)::float / setting::float) * 100) as percent_used
FROM pg_stat_activity, pg_settings 
WHERE name = 'max_connections'
GROUP BY setting;" >> $REPORT_FILE

echo "Report generated: $REPORT_FILE"