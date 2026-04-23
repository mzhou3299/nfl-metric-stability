// =============================================================
// Predictive.scala
// NFL Metric Predictive Validity
//
// PURPOSE:
//   For each of 7 metrics plus a point-differential baseline,
//   and each early window (N=6, N=8):
//   compute the team's value over their first N games, then
//   correlate against two outcome variables over games N+1..16:
//     (1) Win total
//     (2) Point differential
//   Early and late windows never overlap.
//   Bootstrap 1000x for CIs on each correlation.
//
// INPUT:
//   nfl_pbp_1999_2024.csv  — raw data (for scores and results)
//   nfl_pbp_enriched       — cleaned data (metrics + game numbers)
//
// OUTPUT:
//   nfl_predictive_results — one row per (metric, N)
//
// HOW TO RUN:
//   spark-shell --deploy-mode client
//   :load Predictive.scala
// =============================================================

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// =============================================================
// STEP 1: Load raw data and extract game-level results
// We take the last play of each game to get final scores.
// This gives us one row per game with home/away final scores.
// =============================================================

val raw = spark.read.option("header", "true").option("inferSchema", "true").csv("nfl_pbp_1999_2024.csv")

// Filter to regular season only
val rawReg = raw.filter(col("season_type") === "REG")

// Get the last play of each game (highest play_id per game_id)
// That play will have the final score in total_home_score / total_away_score
val lastPlayWindow = Window.partitionBy("game_id").orderBy(col("play_id").desc)

val gameResults = rawReg.withColumn("rn", row_number().over(lastPlayWindow)).filter(col("rn") === 1).select("game_id", "season", "week", "home_team", "away_team", "total_home_score", "total_away_score").withColumn("home_point_diff", col("total_home_score") - col("total_away_score")).withColumn("away_point_diff", col("total_away_score") - col("total_home_score")).withColumn("home_win", when(col("total_home_score") > col("total_away_score"), 1).otherwise(0)).withColumn("away_win", when(col("total_away_score") > col("total_home_score"), 1).otherwise(0))

println(s"Game results rows: ${gameResults.count()}")
gameResults.show(5)

// =============================================================
// STEP 2: Pivot to team-game level
// Each game produces two rows: one for home team, one for away.
// This gives us (season, week, team, win, point_diff) per game.
// =============================================================

val homeRows = gameResults.select(col("game_id"), col("season"), col("week"), col("home_team").alias("team"), col("home_win").alias("win"), col("home_point_diff").alias("point_diff"))

val awayRows = gameResults.select(col("game_id"), col("season"), col("week"), col("away_team").alias("team"), col("away_win").alias("win"), col("away_point_diff").alias("point_diff"))

val teamGameResults = homeRows.union(awayRows)

println(s"Team-game results rows: ${teamGameResults.count()}")
teamGameResults.show(5)

// =============================================================
// STEP 3: Assign game numbers to results
// Same dense_rank approach as Stability.scala so game numbers
// match between metric data and outcome data.
// =============================================================

val gameNumWindow = Window.partitionBy("season", "team").orderBy("week")
val teamGameResultsRanked = teamGameResults.withColumn("game_num", dense_rank().over(gameNumWindow))

println("Sample game-ranked results:")
teamGameResultsRanked.filter(col("team") === "NE" && col("season") === 2019).sort("game_num").show()

// =============================================================
// STEP 4: Load enriched play-by-play data (has game numbers)
// =============================================================

val enriched = spark.read.option("header", "true").option("inferSchema", "true").csv("nfl_pbp_enriched")

// Rebuild game numbers — same logic as Stability.scala
val offGames = enriched.select("season", "posteam", "week", "game_id").distinct()
val offWindow = Window.partitionBy("season", "posteam").orderBy("week")
val offRanked = offGames.withColumn("game_num", dense_rank().over(offWindow))

val defGames = enriched.select("season", "defteam", "week", "game_id").distinct()
val defWindow = Window.partitionBy("season", "defteam").orderBy("week")
val defRanked = defGames.withColumn("game_num", dense_rank().over(defWindow))

val dfOff = enriched.join(offRanked.select("season", "posteam", "game_id", "game_num"), Seq("season", "posteam", "game_id"))
val dfFull = dfOff.join(defRanked.select("season", "defteam", "game_id", "game_num").withColumnRenamed("game_num", "def_game_num"), Seq("season", "defteam", "game_id"))

// Add turnover column
val dfWithTO = dfFull.withColumn("turnover", col("interception") + col("fumble_lost"))

println(s"Enriched with game numbers: ${dfWithTO.count()} rows")

// =============================================================
// STEP 5: Define metrics
// The "point_diff_baseline" metric uses game-level outcomes
// directly and is handled separately below the main loop.
// =============================================================

case class Metric(name: String, teamCol: String, gameNumCol: String, playFilter: String, aggCol: String)

val metrics = Seq(
  Metric("off_pass_epa",  "posteam", "game_num",     "pass", "epa"),
  Metric("off_rush_epa",  "posteam", "game_num",     "rush", "epa"),
  Metric("def_pass_epa",  "defteam", "def_game_num", "pass", "epa"),
  Metric("def_rush_epa",  "defteam", "def_game_num", "rush", "epa"),
  Metric("off_success",   "posteam", "game_num",     "all",  "success"),
  Metric("def_success",   "defteam", "def_game_num", "all",  "success"),
  Metric("turnover_rate", "posteam", "game_num",     "all",  "turnover")
)

// Early window sizes to test — capped at 8 to match stability analysis
val windowSizes = Seq(6, 8)

// =============================================================
// STEP 6: Pearson correlation helper (runs on driver)
// =============================================================

def pearsonCorr(pairs: Array[(Double, Double)]): Double = {
  val n = pairs.length
  if (n < 3) return Double.NaN
  val meanX = pairs.map(_._1).sum / n
  val meanY = pairs.map(_._2).sum / n
  val num = pairs.map { case (x, y) => (x - meanX) * (y - meanY) }.sum
  val denX = math.sqrt(pairs.map { case (x, _) => math.pow(x - meanX, 2) }.sum)
  val denY = math.sqrt(pairs.map { case (_, y) => math.pow(y - meanY, 2) }.sum)
  if (denX == 0 || denY == 0) Double.NaN else num / (denX * denY)
}

// =============================================================
// STEP 7: Bootstrap helper (same as Stability.scala)
// Returns (mean_r, lower_95_ci, upper_95_ci)
// =============================================================

def bootstrap(pairs: Array[(Double, Double)], nIter: Int = 1000, seed: Int = 42): (Double, Double, Double) = {
  val rng = new scala.util.Random(seed)
  val n = pairs.length
  val results = (1 to nIter).map { _ =>
    val sample = Array.fill(n)(pairs(rng.nextInt(n)))
    pearsonCorr(sample)
  }.filter(!_.isNaN).toArray
  if (results.isEmpty) return (Double.NaN, Double.NaN, Double.NaN)
  val sorted = results.sorted
  val mean = results.sum / results.length
  val lower = sorted((results.length * 0.025).toInt)
  val upper = sorted((results.length * 0.975).toInt)
  (mean, lower, upper)
}

// =============================================================
// STEP 8: Main loop — EPA/success metrics
// For each metric and each N (6, 8):
//   1. Aggregate metric over early window (games 1..N)
//   2. Aggregate wins + point diff over late window (N+1..16)
//   3. Join on (season, team)
//   4. Collect to driver
//   5. Bootstrap two correlations: r_wins, r_point_diff
// =============================================================

// Results: (metric, n_games, r_wins, ci_lower_wins, ci_upper_wins, r_point_diff, ci_lower_pd, ci_upper_pd, team_seasons)
var allResults = Seq[(String, Int, Double, Double, Double, Double, Double, Double, Int)]()

for (metric <- metrics) {
  println(s"\n=== Processing metric: ${metric.name} ===")

  val dfFiltered = metric.playFilter match {
    case "pass" => dfWithTO.filter(col("pass") === 1)
    case "rush" => dfWithTO.filter(col("rush") === 1)
    case "all"  => dfWithTO
  }

  val teamGame = dfFiltered.groupBy(col("season"), col(metric.teamCol).alias("team"), col(metric.gameNumCol).alias("game_num")).agg(mean(col(metric.aggCol)).alias("metric_val"))

  for (n <- windowSizes) {

    val earlyMetric = teamGame.filter(col("game_num") <= n).groupBy("season", "team").agg(mean("metric_val").alias("early_metric"))

    val lateOutcomes = teamGameResultsRanked.filter(col("game_num") > n && col("game_num") <= 16).groupBy("season", "team").agg(sum("win").alias("late_wins"), sum("point_diff").alias("late_point_diff"))

    val joined = earlyMetric.join(lateOutcomes, Seq("season", "team"))
    val rows = joined.collect()
    println(s"  N=$n: ${rows.length} team-seasons with both windows")

    val pairsWins = rows.flatMap { row =>
      val m = row.getAs[Double]("early_metric")
      val w = row.getAs[Long]("late_wins").toDouble
      if (m.isNaN) None else Some((m, w))
    }

    val pairsPointDiff = rows.flatMap { row =>
      val m = row.getAs[Double]("early_metric")
      val pd = row.getAs[Long]("late_point_diff").toDouble
      if (m.isNaN) None else Some((m, pd))
    }

    val (rWins, ciLowerWins, ciUpperWins) = bootstrap(pairsWins)
    val (rPointDiff, ciLowerPd, ciUpperPd) = bootstrap(pairsPointDiff)

    println(f"  N=$n: r_wins=$rWins%.3f  [$ciLowerWins%.3f, $ciUpperWins%.3f]  r_point_diff=$rPointDiff%.3f  [$ciLowerPd%.3f, $ciUpperPd%.3f]")

    allResults = allResults :+ (metric.name, n, rWins, ciLowerWins, ciUpperWins, rPointDiff, ciLowerPd, ciUpperPd, rows.length)
  }
}

// =============================================================
// STEP 9: Point differential baseline
// Early point differential (games 1..N) predicting late outcomes.
// This is the naive baseline — if our metrics can't beat this,
// they add no value over what the scoreboard already tells you.
// =============================================================

println(s"\n=== Processing baseline: point_diff_baseline ===")

for (n <- windowSizes) {

  val earlyPd = teamGameResultsRanked.filter(col("game_num") <= n).groupBy("season", "team").agg(sum("point_diff").alias("early_metric"))

  val lateOutcomes = teamGameResultsRanked.filter(col("game_num") > n && col("game_num") <= 16).groupBy("season", "team").agg(sum("win").alias("late_wins"), sum("point_diff").alias("late_point_diff"))

  val joined = earlyPd.join(lateOutcomes, Seq("season", "team"))
  val rows = joined.collect()
  println(s"  N=$n: ${rows.length} team-seasons")

  val pairsWins = rows.flatMap { row =>
    val m = row.getAs[Long]("early_metric").toDouble
    val w = row.getAs[Long]("late_wins").toDouble
    Some((m, w))
  }

  val pairsPointDiff = rows.flatMap { row =>
    val m = row.getAs[Long]("early_metric").toDouble
    val pd = row.getAs[Long]("late_point_diff").toDouble
    Some((m, pd))
  }

  val (rWins, ciLowerWins, ciUpperWins) = bootstrap(pairsWins)
  val (rPointDiff, ciLowerPd, ciUpperPd) = bootstrap(pairsPointDiff)

  println(f"  N=$n: r_wins=$rWins%.3f  [$ciLowerWins%.3f, $ciUpperWins%.3f]  r_point_diff=$rPointDiff%.3f  [$ciLowerPd%.3f, $ciUpperPd%.3f]")

  allResults = allResults :+ ("point_diff_baseline", n, rWins, ciLowerWins, ciUpperWins, rPointDiff, ciLowerPd, ciUpperPd, rows.length)
}

// =============================================================
// STEP 10: Write results
// =============================================================

import spark.implicits._
val resultsDF = allResults.toDF("metric", "n_games", "r_wins", "ci_lower_wins", "ci_upper_wins", "r_point_diff", "ci_lower_pd", "ci_upper_pd", "team_seasons")

println("\n=== Predictive Results ===")
resultsDF.sort("n_games", "metric").show(50, truncate = false)

resultsDF.write.option("header", "true").mode("overwrite").csv("nfl_predictive_results")
println("Results written to HDFS at nfl_predictive_results/")
println("Predictive.scala complete.")
