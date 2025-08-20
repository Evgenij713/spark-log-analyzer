package core

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.time.format.DateTimeFormatter

object LogParser {
  private val LOG_PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)""".r

  def parseRawLogs(spark: SparkSession, rawData: DataFrame): DataFrame = {
    import spark.implicits._

    rawData.flatMap { row =>
        LOG_PATTERN.findFirstMatchIn(row.getString(0)).map { m =>
          (
            m.group(1), // ip
            m.group(4), // timestamp
            m.group(5), // method
            m.group(6), // endpoint
            m.group(7), // protocol
            m.group(8).toInt, // status
            m.group(9).toLong // bytes
          )
        }
      }.toDF("ip", "timestamp", "method", "endpoint", "protocol", "status", "bytes")
      .withColumn("timestamp", to_timestamp(regexp_replace(col("timestamp"), "\\[|\\]", ""), "dd/MMM/yyyy:HH:mm:ss Z"))
  }
}