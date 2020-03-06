package template.spark
import com.twosigma.flint.timeseries.CSV
import com.twosigma.flint.timeseries.TimeSeriesRDD
import org.apache.spark.sql.types._
import scala.concurrent.duration._
import com.twosigma.flint.rdd.function.window
import com.twosigma.flint.timeseries.Summarizers
object Flint extends InitSpark {
  def main(args: Array[String]) = {
    val schema = new StructType()
      .add("time",LongType,true)
      .add("balance",DoubleType,true)
    var df = sqlContext.read.format("com.databricks.spark.csv")
        .option("delimiter", ",")
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(schema)
        .load("file:/D:/spark-gradle-template/dailybalance.csv").toDF()
    df.show()
    val tsRdd = TimeSeriesRDD.fromDF(dataFrame = df)(isSorted = true, timeUnit  = MILLISECONDS  )
    println(tsRdd.getClass)
    val results = tsRdd.groupByCycle()
    results.collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.count()).collect().foreach(println)
    //tsRdd.summarizeCycles(Summarizers.weightedMeanTest("balance","column2")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.mean("balance")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.stddev("balance")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.variance("balance")).collect().foreach(println)
    //tsRdd.summarizeCycles(Summarizers.covariance("balance","column2")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.zScore("balance",true)).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.nthMoment("balance",1)).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.nthCentralMoment("balance",1)).collect().foreach(println)
    //tsRdd.summarizeCycles(Summarizers.correlation("balance","column2")).collect().foreach(println)
    //tsRdd.summarizeCycles(Summarizers.OLSRegression("balance", Seq("column2"))).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.quantile("balance", Seq(0.1, 0.25, 0.5, 0.95))).collect().foreach(println)
    //tsRdd.summarizeCycles(Summarizers.compose(Summarizers.mean("balance"), Summarizers.stddev("balance"))).collect().foreach(println)
    //tsRdd.summarizeCycles(Summarizers.stack( Summarizers.mean("balance"), Summarizers.mean("balance"))).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.exponentialSmoothing("balance")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.min("balance")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.max("balance")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.product("balance")).collect().foreach(println)
    //tsRdd.summarizeCycles(Summarizers.dotProduct("balance", "column2")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.geometricMean("balance")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.skewness("balance")).collect().foreach(println)
    tsRdd.summarizeCycles(Summarizers.kurtosis("balance")).collect().foreach(println)

    close
  }
}
