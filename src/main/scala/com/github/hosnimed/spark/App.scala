package com.github.hosnimed.spark

import com.github.hosnimed.spark.Domain.{Input, schema}
import junit.framework.Assert
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SaveMode}

object App extends App with SparkSessionWrapper {
  Assert.assertNotNull(spark)
  val description =
    """
      |Program arguments :
      |Input CSV (Default : src/main/resources/xag.csv )
      |Output Directory (Default : src/main/resources/)
      |""".stripMargin
  println(description)
  val input_csv = if(args.isEmpty) "src/main/resources/xag.csv" else args(0)
  val path = if(args.isEmpty) "src/main/resources/" else args(1)

  /**
   * Read intial dataset
   */
  val inputDS : Dataset[Input] = loadInputDS(input_csv)
  //Persist initial dataset
  def loadInputDS(input: String) = {
    spark
      .read
      // .option("infer_schema","true")
      .schema(schema)
      .csv(input)
      .as[Input](Encoders.product)
  }

  inputDS.cache
  inputDS.show
  inputDS.printSchema

  import spark.implicits._
  var idx : Int = _
  def increment = {
    idx = idx + 1;
    idx
  }
  /**
   * Define UDF for auto increment
   */
  val inc = udf[Int](() => increment)

  /**
   * filter for users
   * write usersdf to output
   */
  val usersDF: DataFrame = lookupUsers(inputDS)
  def lookupUsers(ds: Dataset[Input]) = {
    // reinitialize the counter
    idx = -1
    // Add new userIdAsInteger column
    val usersDF = ds.select($"userId", $"rating", $"timestamp").withColumn("userIdAsInteger", inc())
    usersDF.cache.show(10)
    // output path
    usersDF.select($"userId", $"userIdAsInteger")
      //      .coalesce(1)
      .write.mode(SaveMode.Overwrite).csv(path + "lookup_user")
    usersDF.unpersist
    usersDF
  }

  /**
   * lookup for products
   * write productsdf to output
   */
  val productsDF: DataFrame = lookupProducts(inputDS)
  def lookupProducts(ds: Dataset[Input]) = {
    // reinitialize the counter
    idx = -1
    // Add new itemIdAsInteger column
    val productsDF: DataFrame = ds.select($"itemId", $"rating", $"timestamp").withColumn("itemIdAsInteger", inc())
    productsDF.cache.show(10)
    // output path
    productsDF.select($"itemId", $"itemIdAsInteger")
      //      .coalesce(1)
      .write.mode(SaveMode.Overwrite).csv(path + "lookup_product")
    productsDF.unpersist
    productsDF
  }
}
