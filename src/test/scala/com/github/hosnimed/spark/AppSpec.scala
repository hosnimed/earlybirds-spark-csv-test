package com.github.hosnimed.spark

import com.github.hosnimed.spark.Domain.Input
import com.github.mrpowers.spark.fast.tests.{DataFrameComparer, DatasetComparerLike}
import org.apache.spark.sql.Dataset
import org.scalatest.FunSpec

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
  }


}