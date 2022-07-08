import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object WindowDataReadWrite extends App{

  case class Customer(country:String,weeknum:Int,numinvoices:Int,totalquantity:Int,invoicevalue:Double)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder.appName("Window Data")
    .master("local[*]")
    .getOrCreate()

  // Reading the file without headers
  val df = spark.read
    .format("csv")
    .option("inferSchema","true")
    .option("path","C://Users//deept//Desktop//Airisdata//datasets//windowdata.csv")
    .load()

  // Adding column names
  val dfWithColumns = df.toDF("country", "weeknum", "numinvoices", "totalquantity", "invoicevalue")

  // Displaying the data
  dfWithColumns.show(10,false);
  dfWithColumns.printSchema()

  // Reading the data with headers
  val dfHeader = spark
    .read
    .format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .option("path","C://Users//deept//Desktop//Airisdata//datasets//windowdata_header.csv")
    .load()

  import spark.implicits._
  val dsHeader = dfHeader.as[Customer]

  // Displaying the data with header
  dfHeader.show(10,false);

  // Each folder with country, week num combination
  dfWithColumns
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("country","weeknum")
    .option("path","C://Users//deept//Desktop//Airisdata//datasets//windowDataWriteOutput")
    .save()

  dfWithColumns
    .write
    .mode("overwrite")
    .format("avro")
    .partitionBy("country")
    .option("path","C://Users//deept//Desktop//Airisdata//datasets//windowDataWriteOutput_avro")
    .save()

  spark.stop();
}