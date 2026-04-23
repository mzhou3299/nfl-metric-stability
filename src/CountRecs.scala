// CountRecs.scala
// Data Profiling for NFL Play-by-Play Data (1999-2024)
// ============================================================
// Load data from HDFS
// ============================================================
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("nfl_pbp_1999_2024.csv")

// ============================================================
// Total record count
// ============================================================
val totalRecords = df.count()
println(s"Total number of records: $totalRecords")

// ============================================================
// Map records to key-value and count using map()
// ============================================================
val mappedCount = df.rdd.map(row => ("record", 1)).reduceByKey(_ + _).collect()
println(s"Mapped record count: ${mappedCount(0)._2}")

// ============================================================
// Distinct values for columns used in analytic
// ============================================================

// Season
println("=== season ===")
df.select("season").distinct().sort("season").show(50)
println("Distinct seasons: " + df.select("season").distinct().count())

// Possession team (offense)
println("=== posteam ===")
df.select("posteam").distinct().sort("posteam").show(50)
println("Distinct posteam: " + df.select("posteam").distinct().count())

// Defensive team
println("=== defteam ===")
df.select("defteam").distinct().sort("defteam").show(50)
println("Distinct defteam: " + df.select("defteam").distinct().count())

// Week
println("=== week ===")
df.select("week").distinct().sort("week").show(30)
println("Distinct weeks: " + df.select("week").distinct().count())

// Play type
println("=== play_type ===")
df.select("play_type").distinct().show(20)
println("Distinct play_types: " + df.select("play_type").distinct().count())

// ============================================================
// Binary column value distributions
// ============================================================
println("=== pass ===")
df.groupBy("pass").count().show()

println("=== rush ===")
df.groupBy("rush").count().show()

println("=== success ===")
df.groupBy("success").count().show()

println("=== interception ===")
df.groupBy("interception").count().show()

println("=== fumble_lost ===")
df.groupBy("fumble_lost").count().show()

// ============================================================
// Null counts for key columns
// ============================================================
val keyCols = Seq("game_id", "season", "week", "posteam", "defteam", "epa", "pass", "rush", "success", "interception", "fumble_lost", "vegas_wp", "wp", "wpa")
println("=== NULL counts ===")
for (c <- keyCols) {
  val nullCount = df.filter(df(c).isNull || df(c) === "" || df(c) === "NA").count()
  println(s"  $c: $nullCount nulls/empty/NA")
}

// ============================================================
// EPA statistics
// ============================================================
println("=== EPA stats ===")
df.select("epa").summary("min", "max", "mean", "stddev", "count").show()

// ============================================================
// Season type check
// ============================================================
df.select("season_type").distinct().show()

// ============================================================
// Re-run profiling on CLEANED data
// ============================================================
val cleanedDf = spark.read.option("header", "true").option("inferSchema", "true").csv("nfl_pbp_cleaned")
println(s"Cleaned record count: ${cleanedDf.count()}")

val mappedClean = cleanedDf.rdd.map(row => ("record", 1)).reduceByKey(_ + _).collect()
println(s"Mapped cleaned count: ${mappedClean(0)._2}")

println("=== Cleaned: season ===")
cleanedDf.select("season").distinct().sort("season").show(50)
println("Distinct seasons: " + cleanedDf.select("season").distinct().count())

println("=== Cleaned: posteam ===")
cleanedDf.select("posteam").distinct().sort("posteam").show(50)
println("Distinct posteam: " + cleanedDf.select("posteam").distinct().count())

println("=== Cleaned: defteam ===")
cleanedDf.select("defteam").distinct().sort("defteam").show(50)
println("Distinct defteam: " + cleanedDf.select("defteam").distinct().count())
