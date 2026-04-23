// =============================================================
// FirstCode.scala
// NFL Play-by-Play Data — Analysis & Additional Cleaning
//
// PURPOSE:
//   This file performs:
//     (1) Descriptive statistics on the cleaned NFL dataset
//         (mean, median, mode, standard deviation)
//     (2) Two additional cleaning steps applied on top of
//         the pipeline already defined in Clean.scala:
//           [CLEANING 1] Text formatting — normalize team codes to uppercase
//           [CLEANING 2] Binary column — create positive_epa flag from epa value
//
// DATA SOURCE:
//   Cleaned data written to HDFS by Clean.scala:
//     Path: /user/<hdfs_user>/nfl_pbp_cleaned/
//     Format: CSV with header, 21 part files
//     Rows: 811,977 (verified via Hive COUNT(*))
//     Columns (13): game_id, season, week, posteam, defteam,
//                   pass, rush, epa, success, interception,
//                   fumble_lost, qb_dropback, vegas_wp, wp, wpa
//
// HOW TO RUN:
//   spark-shell --deploy-mode client
//   :load FirstCode.scala
// =============================================================

import org.apache.spark.sql.functions._

// =============================================================
// STEP 1: Load cleaned data from HDFS
// =============================================================

val df = spark.read.option("header", "true").option("inferSchema", "true").csv("nfl_pbp_cleaned")

println(s"Loaded record count: ${df.count()}")

df.printSchema()

// =============================================================
// [CLEANING 1] TEXT FORMATTING
// Normalize posteam and defteam to UPPERCASE and trim whitespace.
// Ensures consistent team codes across all 26 seasons for
// future GROUP BY and JOIN operations.
// =============================================================

val dfUpper = df.withColumn("posteam", upper(trim(col("posteam")))).withColumn("defteam", upper(trim(col("defteam"))))

println("=== Sample after text normalization ===")

dfUpper.select("posteam", "defteam").distinct().sort("posteam").show(10)

// =============================================================
// [CLEANING 2] BINARY COLUMN FROM CONDITION
// Create positive_epa: 1 if epa > 0, else 0.
// Used as a rate stat in the stability analysis.
// =============================================================

val dfClean = dfUpper.withColumn("positive_epa", when(col("epa") > 0, 1).otherwise(0))

println("=== positive_epa distribution ===")

dfClean.groupBy("positive_epa").count().sort("positive_epa").show()

// =============================================================
// STEP 2: MEAN AND STANDARD DEVIATION
// =============================================================

println("=== Mean and Standard Deviation ===")

dfClean.select(mean(col("epa")).alias("mean_epa"), stddev(col("epa")).alias("stddev_epa"), mean(col("wpa")).alias("mean_wpa"), stddev(col("wpa")).alias("stddev_wpa"), mean(col("vegas_wp")).alias("mean_vegas_wp"), stddev(col("vegas_wp")).alias("stddev_vegas_wp"), mean(col("success")).alias("mean_success"), stddev(col("success")).alias("stddev_success")).show()

// Standard deviation of EPA explicitly called out
println("=== Standard Deviation of EPA (explicit) ===")

dfClean.select(stddev(col("epa")).alias("stddev_epa")).show()

// =============================================================
// STEP 3: MEDIAN (approximate via percentile_approx)
// Spark has no exact median for distributed data.
// percentile_approx at 0.5 with accuracy=10000 is standard.
// =============================================================

println("=== Median (approximate) ===")

dfClean.select(percentile_approx(col("epa"), lit(0.5), lit(10000)).alias("median_epa"), percentile_approx(col("wpa"), lit(0.5), lit(10000)).alias("median_wpa"), percentile_approx(col("vegas_wp"), lit(0.5), lit(10000)).alias("median_vegas_wp"), percentile_approx(col("success"), lit(0.5), lit(10000)).alias("median_success")).show()

// =============================================================
// STEP 4: MODE
// No native mode in Spark — computed via groupBy + count + sort.
// =============================================================

println("=== Mode of success ===")

dfClean.groupBy("success").count().sort(col("count").desc).limit(1).show()

println("=== Mode of positive_epa ===")

dfClean.groupBy("positive_epa").count().sort(col("count").desc).limit(1).show()

println("=== Mode of EPA (rounded to 1 decimal) ===")

dfClean.withColumn("epa_rounded", round(col("epa"), 1)).groupBy("epa_rounded").count().sort(col("count").desc).limit(1).show()

// =============================================================
// STEP 5: FULL SUMMARY STATISTICS
// =============================================================

println("=== Full Summary Statistics ===")

dfClean.select(col("epa"), col("wpa"), col("vegas_wp"), col("success")).summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max").show()

// =============================================================
// STEP 6: TEAM-SEASON AGGREGATION
// Aggregates to the unit of analysis needed for the stability
// analysis — mean metrics per team per season.
// =============================================================

println("=== Team-Season Mean EPA (preview) ===")

val teamSeason = dfClean.groupBy("season", "posteam").agg(count("*").alias("plays"), mean("epa").alias("mean_epa"), mean("success").alias("success_rate"), mean("positive_epa").alias("positive_epa_rate"), mean("interception").alias("int_rate"), mean("fumble_lost").alias("fumble_rate"), (mean("interception") + mean("fumble_lost")).alias("turnover_rate")).sort("season", "posteam")

teamSeason.show(20)

println(s"Total team-season rows: ${teamSeason.count()}")

// =============================================================
// STEP 7: WRITE OUTPUTS TO HDFS
// =============================================================

dfClean.write.option("header", "true").mode("overwrite").csv("nfl_pbp_enriched")

println("Enriched data written to HDFS at nfl_pbp_enriched/")

teamSeason.write.option("header", "true").mode("overwrite").csv("nfl_team_season_stats")

println("Team-season aggregation written to HDFS at nfl_team_season_stats/")

println("FirstCode.scala complete.")
