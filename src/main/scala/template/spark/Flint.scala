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
      .add("id",IntegerType,true)
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
    tsRdd.summarizeCycles(Summarizers.sum("volume")).collect().foreach(println)
    // tsRdd.summarizeCycles(Summarizers.count()).collect().foreach(println)

    // /* Calculates the weighted mean, weighted deviation, weighted t-stat, and the count of observations */
    //  tsRdd.summarizeCycles(Summarizers.weightedMeanTest("volume","weight column")).collect().foreach(println)
    
    // tsRdd.summarizeCycles(Summarizers.mean("volume")).collect().foreach(println)
    // /* Calculates the variance for a column */
    // tsRdd.summarizeCycles(Summarizers.stddev("volume")).collect().foreach(println)
    // /* Calculates the covariance between two columns */
    // tsRdd.summarizeCycles(Summarizers.variance("volume")).collect().foreach(println)
    // /*  Calculates the covariance between two columns */ 
    // tsRdd.summarizeCycles(Summarizers.covariance("volume","v2")).collect().foreach(println)
    //  /* Computes the z-score with the option for out-of-sample calculation */
    // tsRdd.summarizeCycles(Summarizers.zScore("volume",true)).collect().foreach(println)
    // /*  Compute the n-th moment */
    // tsRdd.summarizeCycles(Summarizers.nthMoment("volume", 2)).collect().foreach(println)
    // /* Compute the n-th central moment */
    // tsRdd.summarizeCycles(Summarizers.nthCentralMoment("volume",2)).collect().foreach(println)
    // /* Compute correlations for all possible pairs in `columns` */
    // tsRdd.summarizeCycles(Summarizers.correlation("volume","v2")).collect().foreach(println)
    // /* Binh phuong toi thieu */
    // tsRdd.summarizeCycles(Summarizers.OLSRegression("volume", Seq("v2"))).collect().foreach(println)
    // /* Return a list of quantiles for a given list of quantile probabilities */
    // tsRdd.summarizeCycles(Summarizers.quantile("volume", Seq(0.1, 0.25, 0.5, 0.95))).collect().foreach(println)
    // /* Return a summarizer that is composed of multiple summarizers */
    // tsRdd.summarizeCycles(Summarizers.compose(Summarizers.mean("volume"), Summarizers.stddev("volume"))).collect().foreach(println)
    // /**
    // * Return a summarizer which outputs a single column, `stack`, which contains the results of the summarizer
    // * in the order that they were provided.
    // * The summarizers must contain identical schemas for this to work.
    // * Each summarizer produces one row in the output array.
    // **/
    // val predicate1: Int => Boolean = id => id == 3
    // val predicate2: Int => Boolean = id => id == 7

    // tsRdd.summarizeCycles(Summarizers.stack(
    //     Summarizers.mean("volume").where(predicate1)("id"),
    //     Summarizers.mean("volume").where(predicate2)("id"),
    //     Summarizers.mean("volume")
    // )).collect().foreach(println)

    // /* Performs single exponential smoothing over a column. Primes the EMA by maintaining two EMAs, one over the series (0.0, x_1, x_2, ...) and one over the series (0.0, 1.0, 1.0, ...). For Convolution and Core, the injected zero-valued term is set to be primingPeriods before the first term x_1. For Legacy, it is set to be at time 0.
    // */
    // tsRdd.summarizeCycles(Summarizers.exponentialSmoothing("volume")).collect().foreach(println)
    // tsRdd.summarizeCycles(Summarizers.ewma("volume")).collect().foreach(println)
    // tsRdd.summarizeCycles(Summarizers.min("volume")).collect().foreach(println)
    // tsRdd.summarizeCycles(Summarizers.max("volume")).collect().foreach(println)
    // /* Calculate product X1*X2*X3... */
    // tsRdd.summarizeCycles(Summarizers.product("volume")).collect().foreach(println)
    // /* Tich vo huong giua 2 cot */
    // tsRdd.summarizeCycles(Summarizers.dotProduct("volume", "v2")).collect().foreach(println)
    // /* Trung bình nhân */
    // tsRdd.summarizeCycles(Summarizers.geometricMean("volume")).collect().foreach(println)
    // /* Độ lệch skewness */
    // tsRdd.summarizeCycles(Summarizers.skewness("volume")).collect().foreach(println)
    // /* Độ nhọn kurtosis */
    // tsRdd.summarizeCycles(Summarizers.kurtosis("volume")).collect().foreach(println)
    close
  }
}
