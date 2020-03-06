package template.spark
import com.twosigma.flint.timeseries.CSV
import com.twosigma.flint.timeseries.TimeSeriesRDD
import org.apache.spark.sql.types._
import scala.concurrent.duration._
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.Summarizers
import com.twosigma.flint.timeseries.summarize.summarizer.SumSummarizer
object Flint extends InitSpark {
  def main(args: Array[String]) = {
    val file = "file:/E:/spark-gradle-template/Volume.csv"
    val schema = new StructType()
      .add("time",LongType,true)
      .add("volume",DoubleType,true)
      .add("v2",DoubleType,true)
    var df = sqlContext.read.format("com.databricks.spark.csv")
        .option("delimiter", ",")
        .option("header", "true")
        .schema(schema)
        .load(file).toDF()
    df.show()
    val tsRdd = TimeSeriesRDD.fromDF(dataFrame = df)(isSorted = true, timeUnit  = MILLISECONDS  )
    val results = tsRdd.groupByCycle()

    results.collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.count()).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.weightedMeanTest("volume","v2")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.mean("volume")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.stddev("volume")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.variance("volume")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.covariance("volume","v2")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.zScore("volume",true)).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.nthMoment("volume",1)).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.nthCentralMoment("volume",1)).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.correlation("volume","v2")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.OLSRegression("volume", Seq("v2"))).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.quantile("volume", Seq(0.1, 0.25, 0.5, 0.95))).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.compose(Summarizers.mean("volume"), Summarizers.stddev("volume"))).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.stack( Summarizers.mean("volume"), Summarizers.mean("volume"))).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.exponentialSmoothing("volume")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.min("volume")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.max("volume")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.product("volume")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.dotProduct("volume", "v2")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.geometricMean("volume")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.skewness("volume")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.kurtosis("volume")).collect().foreach(println)
    close
  }
}
