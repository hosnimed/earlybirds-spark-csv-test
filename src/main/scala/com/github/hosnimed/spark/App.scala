package com.github.hosnimed.spark

import junit.framework.Assert

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

}
