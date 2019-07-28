package com.github.hosnimed.spark

import com.github.hosnimed.spark.Domain.Input
import com.github.mrpowers.spark.fast.tests.{DataFrameComparer, DatasetComparerLike}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalatest.FunSpec

import scala.collection.JavaConversions._

class AppSpec
  extends FunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer {

  import spark.implicits._

  val input = "src/test/resources/input/input.csv"
  val output = "src/test/resources/output/"
  val ts: Long = "1564280647844".toLong

  val actualDS: Dataset[Input] = Seq(
    Input("1000", "99", 1.0f, ts),
    Input("1000", "98", 1.1f, ts + 1000),
    Input("1001", "98", 1.2f, ts + 2000),
    Input("1001", "97", 1.3f, ts + 3000),
    Input("1002", "98", 1.3f, ts + 4000),
    Input("1002", "98", 1.2f, ts + 5000),
    Input("1003", "98", 1.1f, ts + 6000)
  ).toDS

  describe("initialize input ds") {

    it("load csv input and convert to ds") {
      val inputDS: Dataset[Input] = App.loadInputDS(input)
      DatasetComparerLike.naiveEquality(inputDS, actualDS)
    }

    it("lookup for users") {
      val actualUsersDF: DataFrame = Seq(
        ("1000", 0),
        ("1000", 1),
        ("1001", 2),
        ("1001", 3),
        ("1002", 4),
        ("1002", 5),
        ("1003", 6)
      ).toDF("userId", "userIdAsInteger")

      val usersDF = App.lookupUsers(actualDS)
      DatasetComparerLike.naiveEquality(usersDF, actualUsersDF)
    }

    it("lookup for products") {
      val actualProductsDF: DataFrame = Seq(
        ("99", 0),
        ("98", 1),
        ("98", 2),
        ("97", 3),
        ("98", 4),
        ("98", 5),
        ("98", 6)
      ).toDF("itemId", "itemIdAsInteger")

      val productDF = App.lookupProducts(actualDS)
      DatasetComparerLike.naiveEquality(productDF, actualProductsDF)
    }
  }

  describe("compute aggregate rating") {

    it("compute max timestamp") {
      val maxts = App.computeMaxTimestamp(actualDS)
      maxts === ts + 6000
    }

    it("apply rating penalty udf") {
      val df = App.applyComputeRatingPenaltyUDF(actualDS, ts + 6000)
      val rows = df.select($"ratingPenalty").collectAsList()
      rows.size() === 7
    }

    it("compute aggregate rating") {
      // just rename the actual rating
      val rpDS = actualDS.withColumnRenamed("rating", "ratingPenalty")
      val df = App.aggregateSumFilter(rpDS)
      val sum = rpDS.select("ratingPenalty").collectAsList().map(r => r.getFloat(0)).sum
      sum === (1.0f + 2 * 1.1f + 2 * 1.2f + 2 * 1.3f)
    }
  }

}