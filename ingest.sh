#!/usr/bin/env bash
# =============================================================
# data_ingest/ingest.sh
# NFL Play-by-Play Data Ingestion
#
# SOURCE: nflFastR play-by-play data, regular seasons 1999-2024
#   Downloaded via R using the nflreadr package:
#     library(nflreadr)
#     pbp <- load_pbp(1999:2024)
#     write.csv(pbp, "nfl_pbp_1999_2024.csv", row.names=FALSE)
#
# The resulting CSV (2.77 GB) was uploaded to HDFS as shown below.
#
# HOW TO RUN (from cluster master node):
#   bash ingest.sh
#
# HDFS location after ingestion:
#   /user/<hdfs_user>/nfl_pbp_1999_2024.csv
# =============================================================

# Upload raw CSV to HDFS home directory
HDFS_USER=$(hdfs dfs -stat "%u" / 2>/dev/null || whoami)
HDFS_PATH="/user/${HDFS_USER}/nfl_pbp_1999_2024.csv"

hdfs dfs -put nfl_pbp_1999_2024.csv "${HDFS_PATH}"

# Verify upload
echo "=== Verifying upload ==="
hdfs dfs -ls "${HDFS_PATH}"
hdfs dfs -du -h "${HDFS_PATH}"

echo "Ingestion complete."
echo "Input data is available on HDFS at: ${HDFS_PATH}"
