import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Connections extends App{

  def extract(row : String)=
  {
    val a = row.split(",")(2).toInt
    val b = row.split(",")(3).toInt
    (a,(b,1))
  }

  val sc = new SparkContext("local[*]","wordcount")
  Logger.getLogger("org").setLevel(Level.ERROR)
  val input = sc.textFile("C:\\Users\\deept\\Desktop\\Airisdata\\datasets\\friendsdataNew.txt")
  val mappedInput = input.map(extract)
  val totalConn  = mappedInput.reduceByKey((x,y) =>(x._1+y._1,x._2+y._2))
  val averageTotal = totalConn.mapValues(x=>x._1/x._2)
  averageTotal.collect.foreach(println)

}
