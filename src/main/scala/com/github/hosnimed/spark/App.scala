package com.github.hosnimed.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object App {

  def happyData()(df: DataFrame): DataFrame = {
    df.withColumn("happy", lit("data is fun"))
  }

}
