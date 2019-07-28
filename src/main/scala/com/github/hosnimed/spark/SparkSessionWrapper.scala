package com.github.hosnimed.spark

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .config("spark.executor.memory", "1g")
    .config("spark.driver.memory", "4g")
    .master("local[*]")
    .appName("spark-csv-test")
    .getOrCreate()
}