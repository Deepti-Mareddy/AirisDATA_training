import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Customers extends App {

  val sc = new SparkContext("local[*]","wordcount")
  Logger.getLogger("org").setLevel(Level.ERROR)
  val input = sc.textFile("C:\\Users\\deept\\Desktop\\Airisdata\\datasets\\customerorders.csv")
  val mappedInput = input.map(x => (x.split(",")(0),x.split(",")(2).toFloat))
  val total = mappedInput.reduceByKey((x,y) =>(x+y))
  val sortedTotal = total.sortBy(x =>x._2,false)
  sortedTotal.take(10).foreach(println)

}
