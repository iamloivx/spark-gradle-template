package template.spark

import org.apache.spark.sql.functions._
import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}

import com.cloudera.sparkts._
import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.util.StatCounter

object Main extends InitSpark {
  def loadObservations(sqlContext: SQLContext, path: String): DataFrame = {
    val rowRdd = sqlContext.sparkContext.textFile(path).map { line =>
      val tokens = line.split('\t')
      val dt = ZonedDateTime.of(tokens(0).toInt, tokens(1).toInt, tokens(2).toInt, 0, 0, 0, 0,
        ZoneId.systemDefault())
      val key = tokens(3).toString
      val price = tokens(5).toDouble
      Row(Timestamp.from(dt.toInstant),key, price)
    }
    val fields = Seq(
      StructField("timestamp", TimestampType, true),
      StructField("key", StringType, true),
      StructField("price", DoubleType, true)
    )
    val schema = StructType(fields)
    sqlContext.createDataFrame(rowRdd, schema)
  }

  def main(args: Array[String]) = {
    val tickerObs = loadObservations(sqlContext, "D:/spark-gradle-template/ticker.tsv")
    val zone = ZoneId.systemDefault()
    val dtIndex = DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(LocalDateTime.parse("2015-08-03T00:00:00"), zone),
      ZonedDateTime.of(LocalDateTime.parse("2015-09-22T00:00:00"), zone),
      new BusinessDayFrequency(1))

    // Align the ticker data on the DateTimeIndex to create a TimeSeriesRDD
    val tickerTsrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, tickerObs,
      "timestamp","key" ,"price")
    // Count the number of series (number of symbols)
    println(tickerTsrdd.count())
    // println(tickerTsrdd.countByValue())
    tickerTsrdd.distinct().foreach(println)
    println(tickerTsrdd.differences(1).count())

    val filled = tickerTsrdd.fill("linear")

    val returnRates = filled.returnRates()

    // Compute Durbin-Watson stats for each series
    val dwStats = returnRates.mapValues(TimeSeriesStatisticalTests.dwtest)

    println(dwStats.map(_.swap).min)
    println(dwStats.map(_.swap).max)

    // stats by key
    val seriesStats = filled.seriesStats()
    seriesStats.collect().foreach(println)
    // get first element 
    println(filled.first()._1)
    println(filled.first()._2)


    close
  }
}
