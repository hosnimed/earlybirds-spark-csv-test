package com.github.hosnimed.spark

import org.apache.spark.sql.types._

object Domain {
  /**
   * Define Data structures
   */
  case class Input(userId: String, itemId: String, rating: Float, timestamp: Long)
  case class User(userId: String, userIdAsInteger: Int, rating: Float, timestamp: Long)
  case class Product(itemId: String, itemIdAsInteger: Int, rating: Float, timestamp: Long)

  /**
   * Define Schema
   */
  val schema = StructType(StructField("userId", StringType, false) ::
    StructField("itemId", StringType, false) ::
    StructField("rating", FloatType, true) ::
    StructField("timestamp", LongType, true) :: Nil)
}
