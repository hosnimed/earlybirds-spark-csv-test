package com.github.hosnimed.spark

import com.github.hosnimed.spark.Domain.{Input, schema}
import junit.framework.Assert
import org.apache.spark.sql.{Dataset, Encoders}

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
}
