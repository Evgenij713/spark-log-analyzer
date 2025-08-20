// Утилиты для Spark
import org.apache.spark.sql.SparkSession

object SparkUtils {
  def createSparkSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")  // Для кластера: .master("spark://<master-url>")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()
  }
}