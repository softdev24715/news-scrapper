#!/bin/bash

# CNTD Parallel Processing Script
# Runs multiple CNTD spider instances in parallel

set -e

# Configuration
BATCH_SIZE=10
MAX_CONCURRENT=3
MAX_PAGES=500
START_PAGE=1
END_PAGE=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
LOG_DIR="logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/cntd_parallel_${TIMESTAMP}.log"

echo -e "${GREEN}Starting CNTD Parallel Processing${NC}"
echo "Log file: $LOG_FILE"
echo "Batch size: $BATCH_SIZE"
echo "Max concurrent: $MAX_CONCURRENT"
echo "Max pages per thematic: $MAX_PAGES"
echo ""

# Load thematic IDs
THEMATIC_FILE="../extra/cntd_sub.json"
if [ ! -f "$THEMATIC_FILE" ]; then
    echo -e "${RED}Error: Thematic file not found: $THEMATIC_FILE${NC}"
    exit 1
fi

# Count total thematics
TOTAL_THEMATICS=$(jq length "$THEMATIC_FILE")
echo "Total thematics: $TOTAL_THEMATICS"

# Calculate number of batches
BATCHES=$(( (TOTAL_THEMATICS + BATCH_SIZE - 1) / BATCH_SIZE ))
echo "Number of batches: $BATCHES"
echo ""

# Create temp directory for batch files
TEMP_DIR="temp_batches"
mkdir -p "$TEMP_DIR"

# Function to create batch file
create_batch() {
    local batch_num=$1
    local start_idx=$(( (batch_num - 1) * BATCH_SIZE ))
    local end_idx=$(( start_idx + BATCH_SIZE - 1 ))
    
    # Extract batch from JSON
    jq ".[$start_idx:$end_idx+1]" "$THEMATIC_FILE" > "$TEMP_DIR/batch_${batch_num:03d}.json"
    
    echo "Created batch $batch_num: thematics $((start_idx + 1))-$((end_idx + 1))"
}

# Function to run spider for a batch
run_spider_batch() {
    local batch_num=$1
    local batch_file="$TEMP_DIR/batch_${batch_num:03d}.json"
    local output_file="cntd_batch_${batch_num:03d}_${TIMESTAMP}.json"
    
    echo -e "${YELLOW}Starting batch $batch_num${NC}" | tee -a "$LOG_FILE"
    
    # Build command
    cmd="scrapy crawl cntd"
    cmd="$cmd -a thematic_ids_file=$batch_file"
    cmd="$cmd -a max_pages_per_thematic=$MAX_PAGES"
    cmd="$cmd -a start_page=$START_PAGE"
    if [ -n "$END_PAGE" ]; then
        cmd="$cmd -a end_page=$END_PAGE"
    fi
    cmd="$cmd -o $output_file"
    
    echo "Command: $cmd" | tee -a "$LOG_FILE"
    
    # Run spider
    if eval "$cmd" >> "$LOG_FILE" 2>&1; then
        echo -e "${GREEN}Batch $batch_num completed successfully${NC}" | tee -a "$LOG_FILE"
        return 0
    else
        echo -e "${RED}Batch $batch_num failed${NC}" | tee -a "$LOG_FILE"
        return 1
    fi
}

# Create all batch files
echo "Creating batch files..."
for ((i=1; i<=BATCHES; i++)); do
    create_batch $i
done
echo ""

# Run batches with limited concurrency
echo "Starting batch processing..."
active_jobs=0
successful=0
failed=0

for ((i=1; i<=BATCHES; i++)); do
    # Wait if we've reached max concurrent jobs
    while [ $(jobs -r | wc -l) -ge $MAX_CONCURRENT ]; do
        sleep 1
    done
    
    # Start new batch
    run_spider_batch $i &
    active_jobs=$((active_jobs + 1))
    
    echo "Started batch $i (active jobs: $active_jobs)"
done

# Wait for all jobs to complete
echo ""
echo "Waiting for all batches to complete..."
wait

# Count results
for ((i=1; i<=BATCHES; i++)); do
    if [ -f "cntd_batch_${i:03d}_${TIMESTAMP}.json" ]; then
        successful=$((successful + 1))
    else
        failed=$((failed + 1))
    fi
done

# Cleanup
echo ""
echo "Cleaning up temporary files..."
rm -rf "$TEMP_DIR"

# Summary
echo ""
echo "=========================================="
echo -e "${GREEN}BATCH PROCESSING COMPLETE${NC}"
echo "Total batches: $BATCHES"
echo -e "Successful: ${GREEN}$successful${NC}"
echo -e "Failed: ${RED}$failed${NC}"
if [ $((successful + failed)) -gt 0 ]; then
    success_rate=$((successful * 100 / (successful + failed)))
    echo "Success rate: $success_rate%"
fi
echo "Log file: $LOG_FILE"
echo "=========================================="

# Exit with error if any batches failed
if [ $failed -gt 0 ]; then
    exit 1
else
    exit 0
fi 