#!/usr/bin/env bash
# ingest.sh - Eco-Commerce ingestion (robust & continues on minor errors)

set -u -o pipefail 

# NEW: Add PostgreSQL bin to PATH so 'psql' can be found
export PATH=$PATH:"/c/Program Files/PostgreSQL/18/bin"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RAW_DIR="${PROJECT_ROOT}/raw_data"
STAGING_DIR="${PROJECT_ROOT}/staging"
ARCHIVE_BASE="${PROJECT_ROOT}/archive"
LOG_DIR="${PROJECT_ROOT}/logs"
LOG_FILE="${LOG_DIR}/ingest.log"
ALLOWED_EXTENSIONS="csv json xlsx"

# Connection variables for Phase 8 Logging
DB_HOST="127.0.0.1"
DB_PORT="6432"
DB_NAME="eco_warehouse"
DB_USER="postgres"

mkdir -p "$STAGING_DIR" "$ARCHIVE_BASE" "$LOG_DIR"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S') ${1}] ${2}" | tee -a "$LOG_FILE"
}

# --- NEW: Database Logging Functions for Phase 8 ---
log_to_db_start() {
    # Added -q (quiet) to prevent "INSERT 0 1" syntax errors in the terminal
    RUN_ID=$(psql -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -U "$DB_USER" -t -q -c \
    "INSERT INTO ingestion_log (process_name, status) VALUES ('Bash_Ingest_Wrapper', 'RUNNING') RETURNING run_id;" | xargs)
}

log_to_db_end() {
    local final_status=$1
    local err_msg=$2
    psql -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -U "$DB_USER" -c \
    "UPDATE ingestion_log SET end_time = CURRENT_TIMESTAMP, status = '$final_status', \
    files_moved = $files_moved, files_invalid = $files_invalid, error_message = '$err_msg' \
    WHERE run_id = $RUN_ID;"
}

log "INFO" "Starting ingestion run ========================================"
log_to_db_start # Track pipeline execution start

files_moved=0
files_skipped=0
files_invalid=0

dated_folders=("$RAW_DIR"/*/)
if [[ ${#dated_folders[@]} -eq 0 || ! -d "${dated_folders[0]}" ]]; then
    log "WARN" "No dated folders found. Exiting."
    log_to_db_end "SUCCESS" "No folders found"
    exit 0
fi

log "DEBUG" "Found ${#dated_folders[@]} dated folders"

for day_dir in "${dated_folders[@]}"; do
    [[ ! -d "$day_dir" ]] && continue

    log "INFO" "Processing: $(basename "$day_dir") ($(ls -1 "$day_dir" 2>/dev/null | wc -l) files)"

    for file in "$day_dir"/*; do
        [[ ! -f "$file" ]] && continue

        filename=$(basename "$file")
        extension="${filename##*.}"
        extension=$(echo "$extension" | tr '[:upper:]' '[:lower:]')

        if [[ ! -s "$file" ]]; then
            log "ERROR" "Empty file skipped: $filename"
            ((files_invalid++))
            continue
        fi

        if ! echo "$ALLOWED_EXTENSIONS" | grep -qw "$extension"; then
            log "ERROR" "Invalid extension skipped: $filename"
            ((files_invalid++))
            continue
        fi

        if [[ "$extension" == "csv" ]]; then
            if ! head -n 1 "$file" 2>/dev/null | grep -q ","; then
                log "WARN" "CSV may be malformed (no comma): $filename"
            fi
        fi

        target="${STAGING_DIR}/${filename}"
        if [[ -f "$target" ]]; then
            log "WARN" "Already in staging, skipping: $filename"
            ((files_skipped++))
            continue
        fi

        mv "$file" "$target" && {
            log "INFO" "Moved: $filename"
            ((files_moved++))
        } || {
            log "ERROR" "Failed to move $filename"
            ((files_invalid++))
        }
    done

    rmdir "$day_dir" 2>/dev/null || true
done

log "INFO" "Summary: Moved=${files_moved} Skipped=${files_skipped} Invalid=${files_invalid}"

if [[ $files_moved -eq 0 ]]; then
    log "INFO" "No new files processed. Exiting."
    log_to_db_end "SUCCESS" "No new files"
    exit 0
fi

# ========================================
# Run ETL pipeline
# ========================================
log "INFO" "Running ETL pipeline on newly moved staging data..."

if python -m etl.pipeline; then
    log "SUCCESS" "ETL pipeline completed successfully"
    ETL_STATUS="SUCCESS"
else
    log "ERROR" "ETL pipeline failed — see above logs for details"
    ETL_STATUS="FAILED"
fi

# ========================================
# Archive processed files
# ========================================
archive_subdir="${ARCHIVE_BASE}/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$archive_subdir"

if ls "$STAGING_DIR"/* >/dev/null 2>&1; then
    mv "$STAGING_DIR"/* "$archive_subdir"/
    log "INFO" "Archived to: $(basename "$archive_subdir") ($(ls "$archive_subdir" | wc -l) files)"
else
    log "WARN" "Nothing in staging to archive (ETL may have cleared it)"
fi

# Final Database Update for Phase 8
if [[ "$ETL_STATUS" == "SUCCESS" ]]; then
    log_to_db_end "SUCCESS" "None"
    log "SUCCESS" "Ingestion + ETL complete. Processed ${files_moved} files."
else
    log_to_db_end "FAILED" "ETL pipeline failed"
    log "ERROR" "Process finished with ETL errors."
fi

echo "Ingestion complete — see $LOG_FILE for details"
exit 0