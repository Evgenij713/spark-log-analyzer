package core

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Sessionizer {
  def createSessions(logs: DataFrame, timeoutMinutes: Int = 30): DataFrame = {
    val windowSpec = Window.partitionBy("ip").orderBy("timestamp")

    logs
      .withColumn("prev_time", lag("timestamp", 1).over(windowSpec))
      .withColumn("diff", (unix_timestamp(col("timestamp")) - unix_timestamp(col("prev_time"))) / 60)
      .withColumn("new_session", when(col("diff").isNull || col("diff") > timeoutMinutes, 1).otherwise(0))
      .withColumn("session_id", sum("new_session").over(windowSpec))
      .groupBy("ip", "session_id")
      .agg(
        min("timestamp").alias("session_start"),
        max("timestamp").alias("session_end"),
        count("*").alias("events_count"),
        collect_list(struct(col("timestamp"), col("method"), col("endpoint"), col("status"))).alias("events")
      )
  }
}