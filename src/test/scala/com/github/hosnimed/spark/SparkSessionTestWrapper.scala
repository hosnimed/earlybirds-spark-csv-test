package com.github.hosnimed.spark

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("spark-csv-test")
    .getOrCreate()

}
