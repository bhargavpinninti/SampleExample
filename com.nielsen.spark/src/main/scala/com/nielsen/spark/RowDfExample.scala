package com.nielsen.spark


import org.apache.spark.sql.types.{ StructType, StructField, StringType }
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.{ SparkSession, DataFrame, Row,Dataset }
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.slf4j.LazyLogging

object RowDfExample extends LazyLogging {
  /* Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)*/
 //val logger :Logger = LoggerFactory.getLogger(rowToDF)
//val logger1 = LoggerFactory.getLogger(rowToDF.getClass)

def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local").appName("app name").getOrCreate()
    
    import spark.implicits._
    /*if(logger.isDebugEnabled())*/
    logger.debug("Here goes my debug message.")
    logger.info("collecting access properties")
  println("===============================")
  
    val sc = spark.sparkContext
    val actuals_date_id = 20181023
    val forecast_date_id = 20180921
    val rowsRdd = sc.parallelize(
      Array(
        Row("Actual", null, "Final", null, s"$actuals_date_id", s"$forecast_date_id")))
 
    val schema = StructType(
      StructField("col1", StringType, true) ::
        StructField("col2", StringType, true) ::
        StructField("col3", StringType, true) ::
        StructField("col4", StringType, true) ::
        StructField("col5", StringType, true) ::
        StructField("col6", StringType, true) :: Nil)

    val df = spark.createDataFrame(rowsRdd, schema)
 
    df.show()
 
  }
}