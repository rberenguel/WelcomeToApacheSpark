// The example in the presentation is simplified for exposition purposes. This
// is the runnable code in spark-shell

import org.apache.spark.sql.types._

spark.read
  .option("header", true)
  .csv("shot_logs.csv")
  .filter('PTS !== 0)
  .select('player_name, 'SHOT_DIST.cast(DoubleType).alias("SHOT_DIST"), 'PTS.cast(IntegerType).alias("PTS"))
  .groupBy('player_name).agg(round(avg('SHOT_DIST), 2), count('SHOT_DIST), sum('PTS))
  .sort(desc("sum(PTS)"))
  .show


import org.apache.spark.sql.types._

spark.read
  .option("header", true)
  .csv("/Users/ruben/Downloads/shot_logs.csv")
  .filter('PTS !== 0)
  .select('player_name, 'SHOT_DIST.cast(DoubleType).alias("SHOT_DIST"), 'PTS.cast(IntegerType).alias("PTS"))
  .groupBy('player_name).agg(round(avg('SHOT_DIST), 2), count('SHOT_DIST), sum('PTS))
  .sort(desc("sum(PTS)"))
  .show

import org.apache.spark.sql.types._

import org.apache.spark.sql.functions.rand


spark.read
  .option("header", true)
  .csv("/Users/ruben/Downloads/shot_logs.csv")
  .drop("GAME_ID")
  .drop("MATCHUP")
  .drop("LOCATION")
  .drop("CLOSEST_DEFENDER_PLAYER_ID")
  .drop("player_id")
  .drop("SHOT_RESULT")
  .drop("PERIOD")
  .drop("FINAL_MARGIN")
  .sort(rand())
  .show(30)


