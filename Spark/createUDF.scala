import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, udf}

object createUDF extends App{

  def ageCheck(age:Int):String={
    if (age>=18) "T" else "F"
  }

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
    .option("header",true)
    .option("path","C:\\Users\\deept\\Desktop\\Airisdata\\datasets\\sessionDemo.txt")
    .load()

  df.printSchema()
  df.show()

  //Object Expression UDF
  val parseAgeFunc = udf(ageCheck(_:Int):String)
  val df2 = df.withColumn("Adult", parseAgeFunc(col("Age")))
  df2.show()

  //String Expression UDF
  spark.udf.register("parseAgeFunc1",ageCheck(_:Int):String)
  val df3 = df.withColumn("Adult",expr("parseAgeFunc1(Age)"))
  df3.show()

  spark.catalog.listFunctions.filter(x=>x.name=="parseAgeFunc1").show()

  df2.createOrReplaceTempView("AgeAnalysis")
  spark.sql("select Name,Age,Adult from AgeAnalysis").show()

  spark.stop()

}
