import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.UserDefinedFunction

import java.io._
import java.text.SimpleDateFormat
import java.util.Locale

object Spark2_RecruitmentChallenge extends App {

  //Leaving comments can impact performance but for this challenge I thought it would make it more readable
  val hadoopHomeDir = new File("src/main/resources/hadoop-2.7.1").getAbsolutePath
  System.setProperty("hadoop.home.dir", hadoopHomeDir)
  val spark = SparkSession.builder()
    .appName("Spark 2 Recruitment Challenge")
    .master("local[*]")
    .getOrCreate()

  val conversionRate = 0.9
  val gogglePlayStore_CsvPath = "src/main/resources/googleplaystore.csv"
  val gogglePlayStore_User_Reviews_CsvPath = "src/main/resources/googleplaystore_user_reviews.csv"
  val best_apps_CsvPath = "src/main/resources/best_apps.csv"
  val cleanedOutputPath = "src/main/resources/googleplaystore_cleaned"
  val metricsOutputPath = "src/main/resources/googleplaystore_metrics"

  val df0 = spark.read.option("header", "true").option("inferSchema", "true")csv(gogglePlayStore_CsvPath)
  val df1 = spark.read.option("header", "true").option("inferSchema", "true")csv(gogglePlayStore_User_Reviews_CsvPath)

  //Part 1 -----------------------------------------------------------------
  val reviewsDFWithDouble = df1.withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast(DoubleType))

  val reviewsDFWithNoNaN = reviewsDFWithDouble.na.fill(0, Seq("Sentiment_Polarity"))

  val avgSentimentDF = reviewsDFWithNoNaN
    .groupBy("App")
    .agg(avg("Sentiment_Polarity").as("Average_Sentiment_Polarity"))

  val df_1 = avgSentimentDF.withColumn(
    "Average_Sentiment_Polarity",
    when(col("Average_Sentiment_Polarity").isNull, lit(0.0))
      .otherwise(col("Average_Sentiment_Polarity"))
  )

  df_1.show()

  //Part 2 -----------------------------------------------------------------
  val dfWithRatingAsDouble = df0.withColumn("Rating", col("Rating").cast(DoubleType))

  val dfWithNoNan = dfWithRatingAsDouble.na.fill(0.0, Seq("Rating"))

  val filteredAppsDF = dfWithNoNan.filter(col("Rating") >= 4.0).orderBy(desc("Rating"))

  val appsData = filteredAppsDF.collect()
  val columns = filteredAppsDF.columns

  FileUtils.writeDataToCSV(appsData, columns, best_apps_CsvPath, "§")

  val df2 = spark.read.option("header", "true").option("delimiter", "§").csv(best_apps_CsvPath)

  df2.show()

  //Part 3 -----------------------------------------------------------------
  val genresConverter = udf((genres: String) => genres.split(";").map(_.trim).toSeq)

  val convertPriceToEuros = udf((price: String) => {
    val priceStr = price.replace("$", "").replace(",", "")
    try {
      val priceDouble = priceStr.toDouble
      priceDouble * conversionRate
    } catch {
      case e: NumberFormatException => 0.0
    }
  })

  val convertSizeUDF: UserDefinedFunction = udf((size: String) => {
    size.toUpperCase match {
      case p if p.endsWith("M") =>
        try {
          p.dropRight(1).toDouble * 1000000
        } catch {
          case _: NumberFormatException => Double.NaN
        }
      case p if p.endsWith("K") =>
        try {
          p.dropRight(1).toDouble * 1000
        } catch {
          case _: NumberFormatException => Double.NaN
        }
      case p =>
        try {
          p.toDouble
        } catch {
          case _: NumberFormatException => Double.NaN
        }
    }
  })

  val convertDateUDF = udf((date: String) => {
    val inputFormat = new SimpleDateFormat("MMMM d, yyyy", Locale.ENGLISH)
    val outputFormat = new SimpleDateFormat("yyyy-MM-dd")
    try {
      val parsedDate = inputFormat.parse(date)
      outputFormat.format(parsedDate)
    } catch {
      case _: Throwable => null
    }
  })

  val dfTransformed = df0
    .withColumnRenamed("Content Rating", "Content_Rating")
    .withColumnRenamed("Last Updated", "Last_Updated")
    .withColumnRenamed("Current Ver", "Current_Version")
    .withColumnRenamed("Android Ver", "Minimum_Android_Version")
    .withColumn("Reviews", col("Reviews").cast(LongType))
    .withColumn("Rating", col("Rating").cast(DoubleType))
    .withColumn("Genres", genresConverter(col("Genres")))
    .withColumn("Price", convertPriceToEuros(col("Price")))
    .withColumn("Size", convertSizeUDF(col("Size")))
    .withColumn("Last_Updated", convertDateUDF(col("Last_Updated")))

  val df_3 = dfTransformed.groupBy("APP").agg(
    collect_set("Category").alias("Categories"),
    first("Rating").alias("Rating"),  // Adjust aggregation as needed
    first("Reviews").alias("Reviews"),
    first("Size").alias("Size"),
    first("Installs").alias("Installs"),
    first("Type").alias("Type"),
    first("Price").alias("Price"),
    first("Content_Rating").alias("Content_Rating"),
    collect_set("Genres").alias("Genres"),
    first("Last_Updated").alias("Last_Updated"),
    first("Current_Version").alias("Current_Version"),
    first("Minimum_Android_Version").alias("Minimum_Android_Version")
  )

  FileUtils.writeDataToCSV(df_3.collect(), df_3.columns, "src/main/resources/part3.csv", ";")

  df_3.show()

  //Part 4 -----------------------------------------------------------------
  val df4 = df_3.join(df_1, Seq("App"), "left")

  df4.show()

  FileUtils.writeDataToCSV(df4.collect(), df4.columns, "src/main/resources/part4.csv", ";")

  df4.write
    .mode("overwrite")
    .option("compression", "gzip")
    .parquet(cleanedOutputPath)

  //Part 5 -----------------------------------------------------------------
  val explodedGenresDF = df4.withColumn("Genre", explode(col("Genres")))
  val df_4 = explodedGenresDF.groupBy("Genre").agg(
    count("App").alias("Count"),
    avg("Rating").alias("Average_Rating"),
    avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
  )

  FileUtils.writeDataToCSV(df_4.collect(), df_4.columns, "src/main/resources/part5.csv", ";")

  df_4.write
    .mode("overwrite")
    .option("compression", "gzip")
    .parquet(metricsOutputPath)

  df_4.show()
  //END---------------------------------------------------
  spark.stop()
}
object FileUtils {

  //This was done before learning about the use of Hadoop
  def writeDataToCSV(data: Array[Row], columns: Array[String], filePath: String, delimiter: String): Unit = {
    val file = new File(filePath)
    val bw = new BufferedWriter(new FileWriter(file))

    bw.write(columns.mkString(delimiter))
    bw.newLine()

    for (row <- data) {
      bw.write(row.mkString(delimiter))
      bw.newLine()
    }

    bw.close()
  }
}

