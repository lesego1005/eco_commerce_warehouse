#!/bin/bash

# --- Configure ---
PG_BIN="/c/Program Files/PostgreSQL/18/bin"
BACKUP_DIR="/c/pg_backups"
DB_NAME="eco_warehouse"
DB_USER="postgres"
RETENTION_DAYS=14

# --- Create backup directory if it doesn't exist ---
mkdir -p "$BACKUP_DIR"

# --- Timestamp (YearMonthDay_HourMinuteSecond) ---
TS=$(date +"%Y%m%d_%H%M%S")
BACKUP_FILE="$BACKUP_DIR/${DB_NAME}_$TS.dump"

# --- Run pg_dump ---
echo "Starting backup for $DB_NAME..."
"$PG_BIN/pg_dump.exe" -U "$DB_USER" -Fc -Z 9 -f "$BACKUP_FILE" "$DB_NAME"

if [ $? -eq 0 ]; then
    echo "Backup successful: $BACKUP_FILE"
else
    echo "Backup failed!"
    exit 1
fi

# --- Remove old backups (Retention) ---
echo "Cleaning up backups older than $RETENTION_DAYS days..."
find "$BACKUP_DIR" -name "${DB_NAME}*.dump" -type f -mtime +$RETENTION_DAYS -delete

echo "Done."