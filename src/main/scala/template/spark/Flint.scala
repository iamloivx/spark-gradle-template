package template.spark
import com.twosigma.flint.timeseries.CSV

object Flint extends InitSpark {
  def main(args: Array[String]) = {
    val df = sqlContext.read.format("com.databricks.spark.csv")
        .option("delimiter", ",")
        .option("header", "true")
        .option("dateFormat", "MM/dd/yyyy")
        .load("file:/D:/spark-gradle-template/dailybalance.csv")


    close
  }
}
