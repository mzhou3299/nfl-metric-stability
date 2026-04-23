// =============================================================
// OpponentAdjust.scala
// NFL Opponent-Adjusted Metric Stability
//
// PURPOSE:
//   Compare raw vs. opponent-adjusted stability curves for
//   each of 6 metrics (excluding turnover_rate) at N=1 to 8.
//
// OPPONENT ADJUSTMENT METHOD (leave-one-game-out):
//   For each play in game G, the opponent's quality is estimated
//   from all of the opponent's OTHER games that season — not
//   including game G. This avoids look-ahead bias from using
//   the full-season average (which includes the current game).
//
//   adj_off_metric = raw_metric - loo_opp_defense_avg
//   adj_def_metric = raw_metric - loo_opp_offense_avg
//
//   Interpretation: positive residual means you performed better
//   than what the opponent typically allows/generates.
//
// INPUT:  nfl_pbp_enriched (HDFS)
// OUTPUT: nfl_opponent_adjust_results (HDFS)
//
// HOW TO RUN:
//   spark-shell --deploy-mode client
//   :load OpponentAdjust.scala
// =============================================================

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// =============================================================
// STEP 1: Load data and assign game numbers
// =============================================================

val enriched = spark.read.option("header", "true").option("inferSchema", "true").csv("nfl_pbp_enriched")

val dfWithTO = enriched.withColumn("turnover", col("interception") + col("fumble_lost"))

val offGames = dfWithTO.select("season", "posteam", "week", "game_id").distinct()
val offRanked = offGames.withColumn("game_num", dense_rank().over(Window.partitionBy("season", "posteam").orderBy("week")))

val defGames = dfWithTO.select("season", "defteam", "week", "game_id").distinct()
val defRanked = defGames.withColumn("game_num", dense_rank().over(Window.partitionBy("season", "defteam").orderBy("week")))

val dfOff = dfWithTO.join(offRanked.select("season", "posteam", "game_id", "game_num"), Seq("season", "posteam", "game_id"))
val dfFull = dfOff.join(defRanked.select("season", "defteam", "game_id", "game_num").withColumnRenamed("game_num", "def_game_num"), Seq("season", "defteam", "game_id"))

println(s"Loaded: ${dfFull.count()} rows")

// =============================================================
// STEP 2: Compute leave-one-game-out opponent quality
//
// For each (season, team, game_id), LOO quality =
//   (sum of EPA across all season games EXCEPT game_id) /
//   (count of plays across all season games EXCEPT game_id)
//
// This is computed once per play-type per perspective and
// joined back to the play-by-play data by (season, team, game_id).
//
// --- TECHNICAL AUDIT: IMPROVEMENT OVER INITIAL APPROACH ---
// v1 used full-season opponent averages joined by (season, defteam):
//
//   val fsDefPassEpa = dfFull.filter(col("pass") === 1)
//     .groupBy("season", "defteam")
//     .agg(mean("epa").alias("opp_def_pass_epa"))
//   val df1 = dfFull.join(fsDefPassEpa, Seq("season", "defteam"))
//   val dfAdj = df1.withColumn("adj_off_pass_epa",
//     col("epa") - col("opp_def_pass_epa"))
//
// PROBLEM: The full-season average for game G includes game G itself,
// making the adjustment circular. When measuring the Eagles in week 3
// vs. the Cowboys, v1 used a Cowboys quality estimate that included
// week 3 — the very game being adjusted. This added noise to both
// early and late windows, causing adjusted correlations to be LOWER
// than raw for almost every metric (diffs of -0.03 to -0.07).
//
// FIX (this version): LOO subtracts game G's contribution from the
// season total before dividing, so each game's adjustment is based
// solely on the opponent's other 15 games. Diffs collapsed to
// near-zero (-0.01 to -0.03), confirming the v1 degradation was
// entirely a measurement artifact, not a real finding.
// ----------------------------------------------------------
// =============================================================

def computeLOO(df: org.apache.spark.sql.DataFrame, teamCol: String, metricCol: String, filterExpr: Option[String] = None): org.apache.spark.sql.DataFrame = {
  val filtered = filterExpr match {
    case Some(expr) => df.filter(expr)
    case None       => df
  }
  // Per-game sum and count
  val perGame = filtered.groupBy(col("season"), col(teamCol).alias("loo_team"), col("game_id").alias("loo_game_id")).agg(sum(col(metricCol)).alias("game_sum"), count(col(metricCol)).alias("game_count"))

  // Season totals
  val perSeason = perGame.groupBy("season", "loo_team").agg(sum("game_sum").alias("season_sum"), sum("game_count").alias("season_count"))

  // LOO = (season_sum - this_game_sum) / (season_count - this_game_count)
  perGame.join(perSeason, Seq("season", "loo_team")).withColumn("loo_avg", (col("season_sum") - col("game_sum")) / (col("season_count") - col("game_count"))).select(col("season"), col("loo_team").alias(teamCol), col("loo_game_id").alias("game_id"), col("loo_avg"))
}

// LOO defense quality for offensive metrics (defteam perspective)
val looDefPass  = computeLOO(dfFull, "defteam", "epa", Some("pass = 1")).withColumnRenamed("loo_avg", "loo_def_pass_epa")
val looDefRush  = computeLOO(dfFull, "defteam", "epa", Some("rush = 1")).withColumnRenamed("loo_avg", "loo_def_rush_epa")
val looDefSucc  = computeLOO(dfFull, "defteam", "success", None).withColumnRenamed("loo_avg", "loo_def_success")

// LOO offense quality for defensive metrics (posteam perspective)
val looOffPass  = computeLOO(dfFull, "posteam", "epa", Some("pass = 1")).withColumnRenamed("loo_avg", "loo_off_pass_epa")
val looOffRush  = computeLOO(dfFull, "posteam", "epa", Some("rush = 1")).withColumnRenamed("loo_avg", "loo_off_rush_epa")
val looOffSucc  = computeLOO(dfFull, "posteam", "success", None).withColumnRenamed("loo_avg", "loo_off_success")

println("LOO opponent quality computed.")

// =============================================================
// STEP 3: Join LOO values back to plays and compute adjusted metrics
// =============================================================

val df1 = dfFull.join(looDefPass,  Seq("season", "defteam", "game_id"), "left")
val df2 = df1.join(looDefRush,  Seq("season", "defteam", "game_id"), "left")
val df3 = df2.join(looDefSucc,  Seq("season", "defteam", "game_id"), "left")
val df4 = df3.join(looOffPass,  Seq("season", "posteam", "game_id"), "left")
val df5 = df4.join(looOffRush,  Seq("season", "posteam", "game_id"), "left")
val df6 = df5.join(looOffSucc,  Seq("season", "posteam", "game_id"), "left")

// Adjusted columns: raw - opponent_quality
// For offensive metrics: subtract defteam's LOO EPA allowed (what they typically give up)
// For defensive metrics: subtract posteam's LOO EPA generated (what they typically produce)
val dfAdj = df6.withColumn("adj_off_pass_epa", col("epa") - col("loo_def_pass_epa")).withColumn("adj_off_rush_epa", col("epa") - col("loo_def_rush_epa")).withColumn("adj_off_success",  col("success") - col("loo_def_success")).withColumn("adj_def_pass_epa", col("epa") - col("loo_off_pass_epa")).withColumn("adj_def_rush_epa", col("epa") - col("loo_off_rush_epa")).withColumn("adj_def_success",  col("success") - col("loo_off_success"))

println("Adjusted columns created.")
dfAdj.select("season", "posteam", "defteam", "game_id", "epa", "loo_def_pass_epa", "adj_off_pass_epa").show(5)

// =============================================================
// STEP 4: Metric definitions
// turnover_rate excluded — noise control variable, no adjustment
// =============================================================

case class AdjMetric(name: String, teamCol: String, gameNumCol: String, playFilter: String, rawCol: String, adjCol: String)

val metrics = Seq(
  AdjMetric("off_pass_epa", "posteam", "game_num",     "pass", "epa",     "adj_off_pass_epa"),
  AdjMetric("off_rush_epa", "posteam", "game_num",     "rush", "epa",     "adj_off_rush_epa"),
  AdjMetric("off_success",  "posteam", "game_num",     "all",  "success", "adj_off_success"),
  AdjMetric("def_pass_epa", "defteam", "def_game_num", "pass", "epa",     "adj_def_pass_epa"),
  AdjMetric("def_rush_epa", "defteam", "def_game_num", "rush", "epa",     "adj_def_rush_epa"),
  AdjMetric("def_success",  "defteam", "def_game_num", "all",  "success", "adj_def_success")
)

// =============================================================
// STEP 5: Correlation and bootstrap helpers
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
// STEP 6: Main loop — N=1 to 8, raw vs. adjusted stability
// =============================================================

// Results: (metric, n_games, r_raw, ci_lower_raw, ci_upper_raw, r_adj, ci_lower_adj, ci_upper_adj)
var allResults = Seq[(String, Int, Double, Double, Double, Double, Double, Double)]()

for (metric <- metrics) {
  println(s"\n=== Processing metric: ${metric.name} ===")

  val dfFiltered = metric.playFilter match {
    case "pass" => dfAdj.filter(col("pass") === 1)
    case "rush" => dfAdj.filter(col("rush") === 1)
    case "all"  => dfAdj
  }

  val teamGame = dfFiltered.groupBy(col("season"), col(metric.teamCol).alias("team"), col(metric.gameNumCol).alias("game_num")).agg(mean(col(metric.rawCol)).alias("raw_val"), mean(col(metric.adjCol)).alias("adj_val"))

  for (n <- 1 to 8) {
    val early = teamGame.filter(col("game_num") <= n).groupBy("season", "team").agg(mean("raw_val").alias("early_raw"), mean("adj_val").alias("early_adj"))
    val late  = teamGame.filter(col("game_num") > n && col("game_num") <= 16).groupBy("season", "team").agg(mean("raw_val").alias("late_raw"), mean("adj_val").alias("late_adj"))
    val rows  = early.join(late, Seq("season", "team")).collect()

    val rawPairs = rows.flatMap { row => val e = row.getAs[Double]("early_raw"); val l = row.getAs[Double]("late_raw"); if (e.isNaN || l.isNaN) None else Some((e, l)) }
    val adjPairs = rows.flatMap { row => val e = row.getAs[Double]("early_adj"); val l = row.getAs[Double]("late_adj"); if (e.isNaN || l.isNaN) None else Some((e, l)) }

    val (rRaw, ciLowerRaw, ciUpperRaw) = bootstrap(rawPairs)
    val (rAdj, ciLowerAdj, ciUpperAdj) = bootstrap(adjPairs)
    val diff = rAdj - rRaw

    println(f"  N=$n: r_raw=$rRaw%.3f [$ciLowerRaw%.3f, $ciUpperRaw%.3f]  r_adj=$rAdj%.3f [$ciLowerAdj%.3f, $ciUpperAdj%.3f]  diff=$diff%.3f")

    allResults = allResults :+ (metric.name, n, rRaw, ciLowerRaw, ciUpperRaw, rAdj, ciLowerAdj, ciUpperAdj)
  }
}

// =============================================================
// STEP 7: Write results
// =============================================================

import spark.implicits._
val resultsDF = allResults.toDF("metric", "n_games", "r_raw", "ci_lower_raw", "ci_upper_raw", "r_adj", "ci_lower_adj", "ci_upper_adj").withColumn("diff", col("r_adj") - col("r_raw"))

println("\n=== Opponent Adjustment Results ===")
resultsDF.sort("metric", "n_games").show(100, truncate = false)

resultsDF.write.option("header", "true").mode("overwrite").csv("nfl_opponent_adjust_results")
println("Results written to HDFS at nfl_opponent_adjust_results/")
println("OpponentAdjust.scala complete.")
