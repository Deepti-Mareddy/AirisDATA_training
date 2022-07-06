import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionDemo extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","session demo")
  sparkConf.set("spark.master","local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    //.enableHiveSupport()
    .getOrCreate()

  val df = spark.read
    .format("csv")
    .option("inferSchema",true)
    .option("path","C:\\Users\\deept\\Desktop\\Airisdata\\datasets\\sessionDemo.txt")
    .load()

  df.printSchema()
  df.show()

  val df1 = df.toDF("name","age","city")
  df1.printSchema()
  df1.show()

  df.write.mode("overwrite").format("csv").save("C:\\Users\\deept\\Desktop\\Airisdata\\datasets\\writeOutput")
    .option("path","C:\\Users\\deept\\Desktop\\Airisdata\\datasets\\writeOutput")
    .save()

  spark.stop()

}
