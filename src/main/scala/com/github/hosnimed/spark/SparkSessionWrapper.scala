package com.github.hosnimed.spark

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "4g")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
    .master("local[*]")
    .appName("spark-csv-test")
    .getOrCreate()
}