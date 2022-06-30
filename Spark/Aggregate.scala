import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Aggregate extends App{

  val sc = new SparkContext("local[*]","wordcount")
  Logger.getLogger("org").setLevel(Level.ERROR)
  val input = sc.textFile("C:\\Users\\deept\\Downloads\\search_data.txt")
  val words = input.flatMap(x => x.split(" "))
  val wordMap = words.map(x => (x,1))
  val aggregate = wordMap.reduceByKey((x,y) =>(x+y)).sortByKey()
  aggregate.collect.foreach(println)

}
