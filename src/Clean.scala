// Clean.scala
// Data Cleaning for NFL Play-by-Play Data (1999-2024)
// ============================================================
// Step 1: Load raw data from HDFS
// ============================================================
val rawDf = spark.read.option("header", "true").option("inferSchema", "true").csv("nfl_pbp_1999_2024.csv")
println(s"Raw record count: ${rawDf.count()}")
println(s"Raw column count: ${rawDf.columns.length}")

// ============================================================
// Step 2: Select only columns needed for analysis
// ============================================================
val selectedDf = rawDf.select(
  "game_id", "season", "season_type", "week", "posteam", "defteam",
  "play_type", "pass", "rush", "epa", "success",
  "interception", "fumble_lost", "qb_dropback",
  "vegas_wp", "wp", "wpa"
)
println(s"Columns after selection: ${selectedDf.columns.length}")
selectedDf.printSchema()

// ============================================================
// Step 3: Filter to regular season games only
// ============================================================
val regDf = selectedDf.filter($"season_type" === "REG")
println(s"Records after filtering to regular season: ${regDf.count()}")

// ============================================================
// Step 4: Drop rows where key columns are null or NA
// ============================================================
val noNullsDf = regDf.filter(
  $"posteam".isNotNull && $"posteam" =!= "" && $"posteam" =!= "NA" &&
  $"defteam".isNotNull && $"defteam" =!= "" && $"defteam" =!= "NA" &&
  $"epa".isNotNull && $"epa" =!= "" && $"epa" =!= "NA"
)
println(s"Records after dropping nulls/NA: ${noNullsDf.count()}")

// ============================================================
// Step 5: Keep only actual plays (pass or rush)
// ============================================================
val playsDf = noNullsDf.filter($"pass" === 1 || $"rush" === 1)
println(s"Records after filtering to pass/rush plays: ${playsDf.count()}")

// ============================================================
// Step 6: Cap all seasons to 16 games
// NFL expanded to 17 games in 2021, so drop weeks 17+
// to ensure consistent season length across all years
// ============================================================
val cappedDf = playsDf.filter($"week" <= 16)
println(s"Records after 16-game cap: ${cappedDf.count()}")

// ============================================================
// Step 7: Cast columns to proper types and drop helper columns
// ============================================================
val cleanDf = cappedDf
  .withColumn("season", $"season".cast("int"))
  .withColumn("week", $"week".cast("int"))
  .withColumn("pass", $"pass".cast("int"))
  .withColumn("rush", $"rush".cast("int"))
  .withColumn("epa", $"epa".cast("double"))
  .withColumn("success", $"success".cast("int"))
  .withColumn("interception", $"interception".cast("int"))
  .withColumn("fumble_lost", $"fumble_lost".cast("int"))
  .withColumn("qb_dropback", $"qb_dropback".cast("int"))
  .withColumn("vegas_wp", $"vegas_wp".cast("double"))
  .withColumn("wp", $"wp".cast("double"))
  .withColumn("wpa", $"wpa".cast("double"))
  .drop("season_type", "play_type")
cleanDf.printSchema()

// ============================================================
// Step 8: Verification checks
// ============================================================
println(s"Final clean record count: ${cleanDf.count()}")
println(s"Final column count: ${cleanDf.columns.length}")
cleanDf.select(min("season"), max("season")).show()
cleanDf.select(min("week"), max("week")).show()
cleanDf.show(10)

// Verify no nulls survived
cleanDf.filter($"epa".isNull).count()
cleanDf.filter($"posteam".isNull || $"posteam" === "NA").count()

// Verify binary columns are clean
cleanDf.groupBy("pass").count().show()
cleanDf.groupBy("rush").count().show()
cleanDf.groupBy("success").count().show()

// Verify every row is either pass or rush
cleanDf.filter($"pass" === 0 && $"rush" === 0).count()

// Verify per-season counts are reasonable
cleanDf.groupBy("season").count().sort("season").show(30)

// ============================================================
// Step 9: Write cleaned data to HDFS
// ============================================================
cleanDf.write.option("header", "true").mode("overwrite").csv("nfl_pbp_cleaned")
println("Cleaned data written to HDFS")

// ============================================================
// Step 10: Create Hive table
// ============================================================
val hdfsUser = System.getProperty("user.name")
spark.sql(s"CREATE EXTERNAL TABLE IF NOT EXISTS nfl_pbp_cleaned (game_id STRING, season INT, week INT, posteam STRING, defteam STRING, pass INT, rush INT, epa DOUBLE, success INT, interception INT, fumble_lost INT, qb_dropback INT, vegas_wp DOUBLE, wp DOUBLE, wpa DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/${hdfsUser}/nfl_pbp_cleaned'")
spark.sql("DESCRIBE nfl_pbp_cleaned").show(false)
spark.sql("SELECT COUNT(*) AS total_rows FROM nfl_pbp_cleaned").show()
