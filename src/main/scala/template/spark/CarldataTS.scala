package template.spark
import carldata.series.Csv

object Flint extends InitSpark {
  def main(args: Array[String]) = {
    val str =
      "time,value
        2005-01-01T12:34:15,2
        2006-01-01T12:34:15,-4
        2007-01-01T12:34:15,-6
        2008-01-01T12:34:15,9".stripMargin
    val series = Csv.fromString(str).head
    println(series.getClass)
 
    close
  }
}
