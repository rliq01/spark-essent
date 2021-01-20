package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataFramesBasics extends App {

  // creating a SparkSession
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  // reding a DF
  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  // showing a DF
  firstDF.show()
  firstDF.printSchema()

  firstDF.take(10).foreach(println)

  // spark types
  val longType = LongType

  // schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  //obtain a schema
  val carsDFschema = firstDF.schema
  println(carsDFschema)

  // read a DF with your own schema
  val carsDFwithSchema = spark.read
    .format("json")
    .schema(carsDFschema)
    .load("src/main/resources/data/cars.json")

  // create rows by hand
  val myRow = Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

  // create DF from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )
  val manualCarsDF = spark.createDataFrame(cars) // schema auto-inferred

  // note: DFs have schemas. Rows do not.

  // crete DFs with implicits
  import spark.implicits._
  val manualCarsDFWithImplicits = cars.toDF("name", "MPG", "cyl", "displ", "HP", "Weight", "acceleration", "year", "country")

  manualCarsDF.printSchema()
  manualCarsDFWithImplicits.printSchema()


  /***
    * Exercise
    * 1) Create manual DF describing smartphones
    *   - make
    *   - model
    *   - processor
    *   - ram
    *   - storage
    *   - camera megaPixels
    *
    * 2) Read another file from the Data folder. movies. dataset.
    */

  val smartPhones = Seq(
    ("iphone", "11", "A13 Bionic", 4, 64, 12),
    ("iphone", "X", "A13 Bionic", 4, 32, 12),
    ("iphone", "6", "A13 Bionic", 4, 128, 12),
    ("android", "Pixel 4a", "Qualcomm Snapdragon 730G", 4, 32, 12),
    ("android", "Pixel 4a", "Qualcomm Snapdragon 730G", 4, 64, 12),
    ("android", "Pixel 4a", "Qualcomm Snapdragon 730G", 4, 128, 12),
    ("google", "X", "Adreno 620", 4, 32, 12),
    ("google", "X", "Adreno 620", 4, 64, 12)
  )

  val manualSmarphoneDf = smartPhones.toDF("make", "model", "processor", "ram", "storage", "camera resolution")

  manualSmarphoneDf.show()

  val moviesDF = spark.read
    .format("json")
    .load("src/main/resources/data/movies.json")

  moviesDF.printSchema()
  println(moviesDF.count())

}
