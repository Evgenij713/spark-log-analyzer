package core

import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}
import com.typesafe.config.ConfigFactory
import java.io.File
import scala.io.Source
import java.lang.UnsatisfiedLinkError
import java.nio.file.{Files, StandardCopyOption}
import scala.util.Try

object LogAnalyzer {
  def main(args: Array[String]): Unit = {
    // Установите для обхода проблем с Hadoop на Windows
    System.setProperty("hadoop.home.dir", "C:\\Windows")
    System.setProperty("hadoop.native.lib", "false")
    System.setProperty("java.io.tmpdir", "D:/temp")
    System.setProperty("spark.local.dir", "D:/temp/spark")

    // Дополнительные настройки для Windows
    System.setProperty("spark.sql.parquet.output.committer.class",
      "org.apache.spark.sql.parquet.DirectParquetOutputCommitter")
    System.setProperty("spark.sql.sources.partitionOverwriteMode", "dynamic")

    val config = ConfigFactory.load()
    val spark = SparkSession.builder()
      .appName(config.getString("spark.app.name"))
      .master(config.getString("spark.app.master"))
      .config("spark.sql.warehouse.dir", "file:///D:/temp")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .config("spark.hadoop.parquet.enable.summary-metadata", "false")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .getOrCreate()

    try {
      val inputPath = config.getString("spark.app.input-path")
      val outputPath = config.getString("spark.app.output-path")
      val timeoutMinutes = config.getInt("spark.session.timeout-minutes")

      println("=" * 60)
      println("ЗАПУСК АНАЛИЗА ЛОГОВ")
      println("=" * 60)

      // Проверка директории
      printStatus("Проверка директории...")
      val dataDir = new File(inputPath)
      if (!dataDir.exists() || !dataDir.isDirectory) {
        printPrettyMessage("ДАННЫХ НЕТ", s"Директория $inputPath не существует!")
        printPrettyMessage("РЕШЕНИЕ", "Создайте папку data/raw_logs и поместите туда файлы с логами")
        return
      }

      val files = dataDir.listFiles()
      if (files == null || files.isEmpty) {
        printPrettyMessage("ПАПКА ПУСТА", s"В директории $inputPath нет файлов!")
        printPrettyMessage("РЕШЕНИЕ", "Добавьте файлы с логами в формата Apache Common Log Format")
        return
      }

      println(s"   Найдено файлов: ${files.length}")

      // Чтение и парсинг логов
      printStatus("Чтение данных...")

      val supportedExtensions = List(".log", ".csv", ".txt", ".data")
      val logFiles = files
        .filter(_.isFile)
        .filter(file => supportedExtensions.exists(ext => file.getName.toLowerCase.endsWith(ext)))
        .toList

      if (logFiles.isEmpty) {
        printPrettyMessage("НЕТ ПОДХОДЯЩИХ ФАЙЛОВ",
          s"В директории нет файлов с поддерживаемыми расширениями: ${supportedExtensions.mkString(", ")}")
        println("   Найденные файлы:")
        files.foreach(file => println(s"      - ${file.getName}"))
        return
      }

      println(s"   Найдено подходящих файлов: ${logFiles.size}")
      logFiles.foreach(file => println(s"      - ${file.getName} (${file.length()} bytes)"))

      // Чтение всех файлов
      val allLines = logFiles.flatMap { file =>
        try {
          println(s"   Чтение: ${file.getName}")
          val lines = Source.fromFile(file, "UTF-8").getLines().toList
          println(s"      Прочитано строк: ${lines.size}")
          lines
        } catch {
          case e: Exception =>
            println(s"      Ошибка чтения ${file.getName}: ${e.getMessage}")
            List.empty[String]
        }
      }

      if (allLines.isEmpty) {
        printPrettyMessage("ВСЕ ФАЙЛЫ ПУСТЫ", "Файлы есть, но они не содержат данных или не могут быть прочитаны!")
        return
      }

      println(s"   Итого прочитано строк: ${allLines.size}")

      // Создаем DataFrame
      val rawLogs = spark.createDataFrame(allLines.map(Tuple1.apply)).toDF("value")
      val rawCount = rawLogs.count()

      if (rawCount == 0) {
        printPrettyMessage("ФАЙЛЫ ПУСТЫ", "Файлы есть, но они не содержат данных!")
        return
      }

      println(s"   Преобразовано в DataFrame: $rawCount записей")

      printStatus("Парсинг логов...")
      val parsedLogs = LogParser.parseRawLogs(spark, rawLogs)
      val parsedCount = parsedLogs.count()

      if (parsedCount == 0) {
        printPrettyMessage("НЕВЕРНЫЙ ФОРМАТ", "Данные не соответствуют ожидаемому формату логов!")
        printPrettyMessage("ФОРМАТ", """IP - user [timestamp] "method endpoint protocol" status bytes""")
        return
      }

      println(s"   Успешно распаршено записей: $parsedCount")

      // Анализ сессий
      printStatus("Анализ сессий...")
      val sessions = Sessionizer.createSessions(parsedLogs, timeoutMinutes)
      val sessionCount = sessions.count()
      println(s"   Обнаружено сессий: $sessionCount")

      // Показываем пример данных
      println("\n" + "=" * 60)
      println("ПРЕВЬЮ ДАННЫХ (первые 5 сессий)")
      println("=" * 60)
      sessions.show(5, truncate = false)

      // Сохранение результатов с использованием улучшенного safeSave
      printStatus("Сохранение результатов...")
      val success = safeSaveWindows(spark, sessions, outputPath)

      // Итоговая статистика
      println("\n" + "=" * 60)
      if (success) {
        println("АНАЛИЗ ЗАВЕРШЕН УСПЕШНО")
        println("=" * 60)
        println(s"• Обработано записей: $parsedCount")
        println(s"• Выявлено сессий: $sessionCount")
        println(s"• Данные сохранены: $outputPath")
      } else {
        println("АНАЛИЗ ЗАВЕРШЕН С ОШИБКАМИ СОХРАНЕНИЯ")
        println("=" * 60)
        println(s"• Обработано записей: $parsedCount")
        println(s"• Выявлено сессий: $sessionCount")
        println("• Данные НЕ сохранены из-за ошибок файловой системы")
      }
      println("=" * 60)

    } catch {
      case e: Exception =>
        println("!" * 60)
        println("КРИТИЧЕСКАЯ ОШИБКА")
        println("!" * 60)
        e.printStackTrace()
        System.exit(1)
    } finally {
      println("Завершение работы Spark сессии...")
      spark.stop()
      println("Spark сессия завершена")
    }
  }

  /**
   * Улучшенное сохранение для Windows с множественными fallback-стратегиями
   */
  private def safeSaveWindows(spark: SparkSession, df: DataFrame, path: String): Boolean = {
    val attempts = List(
      () => trySaveParquet(df, path),
      () => trySaveJSON(df, path + "_json"),
      () => trySaveCSV(df, path + "_csv"),
      () => trySaveWithJavaIO(df, path + "_javaio")
    )

    attempts.zipWithIndex.foldLeft(false) { case (success, (attempt, index)) =>
      if (success) true
      else {
        println(s"   Попытка ${index + 1}/${attempts.size}:")
        attempt()
      }
    }
  }

  private def trySaveParquet(df: DataFrame, path: String): Boolean = {
    try {
      println(s"   Сохранение в Parquet: $path")

      // Очищаем директорию перед записью
      cleanDirectory(path)

      df.write
        .mode(SaveMode.Overwrite)
        .option("compression", "snappy")
        .parquet(path)

      // Проверяем, что файлы действительно создались
      val outputDir = new File(path)
      if (outputDir.exists() && outputDir.list().exists(_.startsWith("part-"))) {
        println(s"   Успешно сохранено в Parquet: $path")
        true
      } else {
        println(s"   Parquet сохранение завершено, но файлы не найдены")
        false
      }
    } catch {
      case e: UnsatisfiedLinkError =>
        println(s"   Ошибка Windows Hadoop: ${e.getMessage}")
        false
      case e: Exception =>
        println(s"   Ошибка сохранения Parquet: ${e.getMessage}")
        false
    }
  }

  private def trySaveJSON(df: DataFrame, path: String): Boolean = {
    try {
      println(s"   Сохранение в JSON: $path")
      cleanDirectory(path)

      df.write
        .mode(SaveMode.Overwrite)
        .json(path)

      val outputDir = new File(path)
      if (outputDir.exists() && outputDir.list().exists(_.startsWith("part-"))) {
        println(s"   Успешно сохранено в JSON: $path")
        true
      } else {
        false
      }
    } catch {
      case e: Exception =>
        println(s"   Ошибка сохранения JSON: ${e.getMessage}")
        false
    }
  }

  private def trySaveCSV(df: DataFrame, path: String): Boolean = {
    try {
      println(s"   Сохранение в CSV: $path")
      cleanDirectory(path)

      df.write
        .mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv(path)

      val outputDir = new File(path)
      if (outputDir.exists() && outputDir.list().exists(_.startsWith("part-"))) {
        println(s"   Успешно сохранено в CSV: $path")
        true
      } else {
        false
      }
    } catch {
      case e: Exception =>
        println(s"   Ошибка сохранения CSV: ${e.getMessage}")
        false
    }
  }

  private def trySaveWithJavaIO(df: DataFrame, path: String): Boolean = {
    try {
      println(s"   Сохранение через Java IO: $path")

      // Собираем данные в драйвере и сохраняем обычными средствами Java
      val data = df.collect()
      if (data.isEmpty) {
        println("   Нет данных для сохранения")
        return false
      }

      val outputDir = new File(path)
      if (!outputDir.exists()) {
        outputDir.mkdirs()
      }

      // Сохраняем как текстовый файл
      val outputFile = new File(outputDir, "sessions.txt")
      val writer = new java.io.PrintWriter(outputFile, "UTF-8")

      try {
        writer.println("ip,session_id,session_start,session_end,events_count")
        data.foreach { row =>
          val ip = row.getString(0)
          val sessionId = row.getLong(1)
          val sessionStart = row.getTimestamp(2)
          val sessionEnd = row.getTimestamp(3)
          val eventsCount = row.getLong(4)

          writer.println(s"$ip,$sessionId,$sessionStart,$sessionEnd,$eventsCount")
        }
        println(s"   Данные сохранены в: ${outputFile.getAbsolutePath}")
        true
      } finally {
        writer.close()
      }
    } catch {
      case e: Exception =>
        println(s"   Ошибка сохранения через Java IO: ${e.getMessage}")
        false
    }
  }

  private def cleanDirectory(path: String): Unit = {
    Try {
      val dir = new File(path)
      if (dir.exists()) {
        deleteRecursively(dir)
      }
      dir.mkdirs()
    }.recover {
      case e: Exception =>
        println(s"   Предупреждение: не удалось очистить директорию $path: ${e.getMessage}")
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    if (file.exists() && !file.delete()) {
      println(s"   Не удалось удалить: ${file.getAbsolutePath}")
    }
  }

  private def printPrettyMessage(title: String, message: String): Unit = {
    println("\n" + "!" * 60)
    println(s"$title")
    println("!" * 60)
    println(s"$message")
    println("!" * 60)
  }

  private def printStatus(message: String): Unit = {
    println(s"\n$message")
  }
}