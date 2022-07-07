import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

case class Person(Name:String,Age:Int,city:String)

object dataFrameNdatasets extends App {
  val sparkConf=new SparkConf()
  sparkConf.setAppName("First App")
  sparkConf.setMaster("local[2]")

  val spark=SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val df=spark.read
    .format("csv")
    .option("inferSchema",true)
    .option("header",true)
    .option("path","C:\\Users\\deept\\Desktop\\Airisdata\\datasets\\sessionDemo.txt")
    .load()

  //Dataframe to Dataset
  import spark.implicits._
  val ds1=df.as[Person]

  //Dataset to Dataframe
  val df1=ds1.toDF()

  ds1.groupByKey(x=>x.Age)
  ds1.filter(x=>x.Age>18).show()
  df1.filter("Age>18").show()

  //Adding single column to df1
  val df2=df1.withColumn("Age10",col("Age")*10)
  df2.show()

  //Adding multiple columns to df
  df2.createOrReplaceTempView("Student")
  spark.sql("select Name,Age,Age10,City,Age*100 as Age100,'India' as country from Student").show()

  //Modify existing column of df
  df2.withColumn("Age",col("Age")*2).show()

  val df3 = df1.select(col("Name"),col("Age"),(col("Age")*10).as("Age10"),(col("Age")%2).alias("AgeP"),col("City"),lit("India").alias("Country"))
  df3.show();
  spark.stop()


}