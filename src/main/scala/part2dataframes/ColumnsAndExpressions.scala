package part2dataframes

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("dfColumnsAndExpressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  // Columns
  val firstColumn: Column = carsDF.col("Name")

  // selecting (projecting)
  val carNamesDF: DataFrame = carsDF.select(firstColumn)

  carNamesDF.show()

  // various select methods
  import spark.implicits._
  carsDF.select(
    col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbol, auto-converted to column
    $"Horsepower", // fanceier interprlated string, returns a column object
    expr("Origin") // EXPRESSION
  )

  // returns another DF
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  val simplestExpression: Column = carsDF.col("Weight_in_lbs") //column is sub-type of expression
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kgs"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kgs_2")
  )

  carsWithWeightsDF.show()

  // selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )


  // DF processing

  //adding a column
  val carsWithKg3DF= carsDF
    .withColumn(
      "Weight_in_kgs_3",
      col("Weight_in_lbs") / 2.2)

  //renaming a column
  val carsWithColumnRenamed = carsDF
    .withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  //careful with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`").show()
  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val euroCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val euroCarsDF_2 = carsDF.where(col("Origin").notEqual("USA"))
  val americanCarsDf = carsDF.filter("Origin = 'USA'")

  //chain filters
  val americanPowerCarsDF = carsDF
    .filter(col("Horsepower") > 150)
    .filter(col("Origin") === "USA")

  val americanPowerCarsDF_2 = carsDF
    .filter(col("Origin") === "USA" and col("Horsepower") > 150)

  val americanPowerCarsDF_3 = carsDF
    .filter("Origin = 'USA' and Horsepower > 150")

  americanPowerCarsDF_2.show()
  println(s"1: ${americanPowerCarsDF.count()}")
  println(s"2: ${americanPowerCarsDF_2.count()}")
  println(s"3: ${americanPowerCarsDF_3.count()}")

  // unioning = adding more rows
  val moreCarsDF = spark.read
    .option("inferSchema", "ture")
    .json("src/main/resources/data/more_cars.json")

  val allCarsDF = carsDF.union(moreCarsDF) // works if DFs have the same schema

  // distince values
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()

}
