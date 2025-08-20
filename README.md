# spark-log-analyzer
Краткое описание: spark-log-analyzer - это система анализа логов веб-сервера, которая представляет собой распределенное приложение для анализа логов веб-сервера в формате Apache Common Log Format. Основная задача системы - идентификация пользовательских сессий на основе временных интервалов и анализ активности пользователей.

Компоненты системы:
1) LogAnalyzer - основной координатор процесса анализа;
2) LogParser - модуль парсинга сырых лог-записей;
3) Sessionizer - модуль группировки событий в пользовательские сессии.

Используемые технологии:
1) Apache Spark	3.5.6 - Обработка больших данных, распределенные вычисления;
2) Scala 2.12.18 - Основной язык программирования;
3) Hadoop 3.3.6 (в составе Spark) - Работа с файловой системой;
4) apache-maven-3.9.11.

Форматы данных:
1) Входные данные (папка src\main\resources): Apache Common Log Format (текстовые логи);
2) Промежуточные данные: Spark DataFrames;
3) Выходные данные (папка data\processed_javaio): CSV, JSON, Parquet (с fallback на текстовые файлы).

Процесс обработки данных:
1) Чтение и валидация;
2) Парсинг логов;
3) Сессионизация;
4) Сохранение результатов.

Запуск системы:
1) Установить hadoop-3.3.6, spark-3.5.6-bin-hadoop3, apache-maven-3.9.11, winutils совместимый с hadoop-3.3.6;
2) Настроить переменные среды Windows: JAVA_HOME, HADOOP_HOME, MAVEN_HOME, SPARK_HOME, Path;
2) В 1 командной строке запустить: spark-class org.apache.spark.deploy.master.Master;
3) Во 2 командной строке из папки spark-3.5.6-bin-hadoop3 запустить рабочий узел: spark-class org.apache.spark.deploy.worker.Worker spark://192.168.56.1:7077;
4) В 3 командной строке из папки spark-3.5.6-bin-hadoop3 запустить приложение: spark-shell --master spark://192.168.56.1:7077;
5) Проверить, открыв в браузере: http://localhost:8080/ ;
6) В ntelliJ IDEA установить плагин Scala, настроить settings.xml (если нужно);
7) В файле src\main\scala\core\LogAnalyzer.scala настроить пути для временных файлов;
8) В консоли ntelliJ IDEA в папке проекта выполнить команды:
- сборка проекта: mvn clean package или полностью очистить и пересобрать: mvn clean compile package;
- запустить проект: spark-submit --class "core.LogAnalyzer" target/spark-log-analyzer-1.0-SNAPSHOT-jar-with-dependencies.jar.
