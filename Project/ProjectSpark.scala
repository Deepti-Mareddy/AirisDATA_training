import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types._


case class Rental_data(id : Int, property_type : String, room_type : String, bathrooms: Double, bedrooms : Int, minimum_nights : Int, location_id : Int, price : Int)

case class Location (id : Int, city : String,state : String, country : String, pincode : Int)

object ProjectSpark extends App{

  val rentalSchema = new StructType()
    .add("id",IntegerType,nullable = false)
    .add("property_type",StringType,nullable = false)
    .add("room_type",StringType,nullable = false)
    .add("bathrooms",DoubleType,nullable = false)
    .add("bedrooms",IntegerType,nullable = false)
    .add("minimum_nights",IntegerType,nullable = false)
    .add("location_id",IntegerType,nullable = false)
    .add("price",IntegerType,nullable = false)


  def discount(price:Int):Double={
    //To calculate discount

    if (price>=200 && price<300) { "%6.2f".format(price*0.05).toDouble }

    else if (price>=300 && price<500) { "%6.2f".format(price*0.1).toDouble }

    else if (price>=500 && price<1000) { "%6.2f".format(price*0.15).toDouble }

    else if(price>=1000 ) { "%6.2f".format(price*0.20).toDouble }

    else { 0 }
  }

  val sparkConf=new SparkConf()
  sparkConf.setAppName("First App")
  sparkConf.setMaster("local[2]")

  val spark=SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()
    //.enableHiveSupport()

  // Reading file Rental_data
  //val df_rental=spark.sql("select * from rental_data1")

  val df_rental=spark.read
    .format("csv")
    .schema(rentalSchema)
    .option("path","C:\\Users\\deept\\Desktop\\Airisdata\\Rental_data.csv")
    .load()


  // Adding Column names
  val df_columns_rental = df_rental.toDF("id","property_type","room_type","bathrooms","bedrooms","minimum_nights","location_id","price")
  df_columns_rental.printSchema()

  //reading file Location
  //val df_location=spark.sql("select * from Location1")

  val df_location=spark.read
    .format("csv")
    .option("inferSchema",true)
    .option("path","C:\\Users\\deept\\Desktop\\Airisdata\\location.csv")
    .load()

  //Adding Column names
  val df_columns_location = df_location.toDF("id","city","state","country","pincode")
  df_columns_location.printSchema()

  //String Expression UDF for calculating discount
  spark.udf.register("discount_amt",discount(_:Int):Double)
  val df_rental_discount = df_columns_rental.withColumn("Discount",expr("discount_amt(price)"))
  df_rental_discount.show()

  //Adding column price after discount
  val df_rental_final = df_rental_discount.withColumn("FinalPrice",col("price")-col("Discount"))
  df_rental_final.show()

  //Convert rental_data df to dataset
  import spark.implicits._
  val ds_rental=df_rental.as[Rental_data]

  //Rentals with minimum stay greater than 15
  ds_rental.filter(x=>x.minimum_nights>15.0).show()

  //Query to find the states that has rentals in more than 1 city
  df_columns_location.createOrReplaceTempView("Loc")
  spark.sql("select state,count(state) as no_of_cities from Loc group by state having count(state)>1 ").show()

  //Rentals with discount more than $100
  df_rental_final.createOrReplaceTempView("Rental")
  spark.sql("select id,property_type,room_type,price,discount,FinalPrice from Rental where discount>=100").show()

  //Finding the number of rentals for each location_id and displayings its city, state, pincode
  spark.sql("select r.location_id,l.city,l.state,l.pincode,count(r.location_id) as number_of_rentals from Rental r join loc l on r.location_id=l.id group by r.location_id,l.city,l.state,l.pincode order by location_id").show()



}
