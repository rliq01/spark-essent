package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
import part2dataframes.DataFramesBasics.{carsDFschema, moviesDF, spark}

import java.util.Properties

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
  Reading a DF requires :
  - format
  - schema or inferSchema = true
  - zero or more options
   */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast") // dropMalformed, permissive (default)
    .option("path", "src/main/resources/data/cars.json")
    .load()

  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failfast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /*
  Writing DFs
  - format
  - save mode = overwrite, append, ignore, errorIfExists
   */
  //  carsDF.write
  //    .format("json")
  //    .mode(SaveMode.Overwrite)
  //    .option("path", "src/main/resources/data/newCars_DUPE.json")
  //    .save()


  //json flags
  spark.read
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") // couple with schema; if spark fails parsing, it will put null for the date.
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  // CSV flags
  val stocksSchema = StructType(
    Array(
      StructField("symbol", StringType),
      StructField("date", DateType),
      StructField("price", DoubleType)
    )
  )

  val stocksDf = spark.read
    .format("csv")
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .load("src/main/resources/data/stocks.csv")

  // Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars.parquet")

  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  // reading from a remote DB
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()

  /**
    * Exercise: read the movies DF, then write it as
    * - tab-separated values file
    * - snappy Parquet
    * - table in the Postgres DB in a table called public.movies
    */


  val moviesDF = spark.read
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-YY")
    .json("src/main/resources/data/movies.json")

  moviesDF.printSchema()

  moviesDF.write
    .option("header", "true")
    .option("nullValue", "")
    .option("sep", "\t")
    .mode(SaveMode.Overwrite)
    .csv("src/main/resources/data/movies.csv")

  moviesDF.write
    .parquet("src/main/resources/data/movies.parquet")

  val props = new Properties()
  props.setProperty("user", "docker")
  props.setProperty("password", "docker")
  moviesDF.write
    .jdbc(
      url = "jdbc:postgresql://localhost:5432/rtjvm",
      table = "public.movies",
      connectionProperties = props)
}
