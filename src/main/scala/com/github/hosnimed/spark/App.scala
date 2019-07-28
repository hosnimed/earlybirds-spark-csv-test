package com.github.hosnimed.spark

import com.github.hosnimed.spark.Domain.{Input, schema}
import junit.framework.Assert
import org.apache.spark.sql.functions.{lit, max, sum, udf}
import org.apache.spark.sql.types.FloatType
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

  /**
   * Compute Rating Aggregation
   */
  // compute max timestamp
  def computeMaxTimestamp(ds: Dataset[Input]) = {
    ds.agg(max($"timestamp")).head.getLong(0)
  }
  val maxTS = computeMaxTimestamp(inputDS)

  // one day
  val day = 1000 * 3600 * 24
  // function for computing rating penalty
  def computeRatingPenalty(rating: Float, timestamp: Long, maxTimestamp: Long): Float = (rating * Math.pow(0.95, Math.floor((maxTimestamp - timestamp) / day))).toFloat
  // udf for computing rating penalty
  val computeRatingPenaltyUDF = udf(computeRatingPenalty _, FloatType)
  // add rating penalty column
  val ratingPenaltyDF = applyComputeRatingPenaltyUDF(inputDS, maxTS)
  def applyComputeRatingPenaltyUDF(ds: Dataset[Input], maxTimestamp: Long) = {
    ds.withColumn("ratingPenalty", computeRatingPenaltyUDF($"rating", $"timestamp", lit(maxTimestamp)))
  }
  inputDS.unpersist
  ratingPenaltyDF.cache

  // sum rating and filter those greater than 0.01
  val sumRatingDF = aggregateSumFilter(ratingPenaltyDF)
  ratingPenaltyDF.unpersist
  sumRatingDF.cache
  def aggregateSumFilter(df: DataFrame): DataFrame = {
    df.groupBy($"userId", $"itemId").agg(sum($"ratingPenalty")).withColumnRenamed("sum(ratingPenalty)", "ratingPenalty").filter($"ratingPenalty" > 0.01)
  }

  // join with usersDf and productsDf keeping sumRating
  val agg_rating: DataFrame = sumRatingDF
    .join(usersDF, usersDF("userId") === sumRatingDF("userId"), "inner")
    .join(productsDF, productsDF("itemId") === sumRatingDF("itemId"), "inner")
    .select($"userIdAsInteger", $"itemIdAsInteger", $"ratingPenalty")
//    .limit(10000)

  sumRatingDF.unpersist
  agg_rating.cache
  agg_rating.printSchema
  agg_rating.show

  //save agg_rating to output folder
  agg_rating
    //    .coalesce(agg_rating.rdd.partitions.size)
    .write
    .mode(SaveMode.Overwrite)
    .csv(path + "agg_ratings")
  agg_rating.unpersist

}
