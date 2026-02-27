#!/bin/bash
# Alert Thresholds
MAX_BLOAT_RATIO=0.3  # 30%
MAX_CONNECTIONS_PCT=80

# Check Bloat (Dead Tuples)
BLOAT_ALERT=$("/c/Program Files/PostgreSQL/18/bin/psql.exe" -U postgres -d eco_warehouse -t -c "
SELECT count(*) FROM pg_stat_user_tables 
WHERE (n_dead_tup::float / NULLIF(n_live_tup,0)::float) > $MAX_BLOAT_RATIO;")

if [ "${BLOAT_ALERT//[[:space:]]/}" -gt 0 ]; then
    echo -e "\033[0;31m [ALERT] High Table Bloat detected! Run VACUUM ANALYZE. \033[0m"
    # Optional: Send Windows Toast Notification
    powershell.exe -Command "New-BurntToastNotification -Text 'DB Alert', 'High Table Bloat Detected!'"
fi