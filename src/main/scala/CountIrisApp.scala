/**
  * Created by purvag on 5/26/16.
  *
  */
import org.apache.spark.{SparkConf, SparkContext}

object MapFunction {
  def mapIris(line: String): (String, Int) = {
    val elements = line.split(",")
    var iris = "Unknown"
    if (elements.length == 5) {
      iris = elements(4)
    }
    return (iris, 1)
  }

  def sum(ab: (String, Int)): String = ab match {
    case (a, b) => Array(a, b).mkString(",")
  }
}

object ReduceFunction {
  def sum(val1: Int, val2: Int): Int = {
    return val1 + val2
  }
}

object CountIris {

  def main(args: Array[String]) {
    val logFile = "data/iris.data" // Should be some file on your system
    val conf = new SparkConf().setAppName("Hello Application")
    conf.setMaster("local");
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()

    val out = logData.map(MapFunction.mapIris).reduceByKey(ReduceFunction.sum).
      map(MapFunction.sum)

    out.saveAsTextFile("output")
  }
}

