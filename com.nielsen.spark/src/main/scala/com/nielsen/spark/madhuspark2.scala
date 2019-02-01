package com.nielsen.spark

import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.{ SparkSession, DataFrame, Row, Dataset }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.types.StructType
import scala.reflect.api.materializeTypeTag
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Dataset
import java.sql.Date

object madhuspark2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local").appName("app name").config("spark.some.config.option", "True").getOrCreate()
    import spark.implicits._

    var ntnl_ofset_fr_day: Dataset[ntnl_ofset_fr_day1] = null

    ntnl_ofset_fr_day = ntnl_ofset_fr_day1.load(spark, "./input/tv_dstbn_source_noncoded.csv")

    val windowSpec = Window.partitionBy(ntnl_ofset_fr_day.col("tz_id")
      && year(ntnl_ofset_fr_day.col("sttm_utc"))
      && ntnl_ofset_fr_day.col("sttm_utc"))
      .orderBy(ntnl_ofset_fr_day.col("clct_dt_dt").asc)
    val ntnlSerTzOfst = ntnl_ofset_fr_day
      .withColumn("is_elg", dense_rank().over(windowSpec))
      .withColumn("sttm_utc_epoch", unix_timestamp(col("sttm_utc")))
      .withColumn("endtm_utc_epoch", unix_timestamp(col("endtm_utc")))
      .withColumn("sttm_lcl_epoch", unix_timestamp(col("sttm_lcl")))
      .withColumn("endtm_lcl_epoch", unix_timestamp(col("endtm_lcl")))
      .select(
        col("is_elg"),
        col("`type`"),
        col("tz_id"),
        col("clct_dt_dt"),
        col("intb_dt_dt"),
        col("sttm_utc"),
        col("endtm_utc"),
        col("sttm_est"),
        col("endtim_est"),
        col("sttm_lcl"),
        col("endtm_lcl"),
        col("sttm_utc_epoch"),
        col(" endtm_utc_epoch"),
        col(" sttm_lcl_epoch"),
        col(" endtm_lcl_epoch"),
        col("sttm_mvd"),
        col("endtm_mvd"),
        col("est_to_utc_ofst"),
        col("lcl_to_utc_ofst"),
        col("mvd_to_utc_ofst"),
        col("lcl_to_est_ofst"),
        col("mvd_to_est_ofst"),
        col("mvd_to_lcl_ofst"),
        col("lcl_to_mvd_ofst"))

    val Finalresult = ntnlSerTzOfst.filter(col("is_elg") === 1)
      .select(col("`type`"), col("tz_id"), col("clct_dt_dt").alias("dt_dt"), min(col("sttm_utc")).alias("sttm_utc"), max(col("endtm_utc")).alias("endtm_utc"), min(col("sttm_utc_epoch")).alias("sttm_utc_epoch"), max(col("endtm_utc_epoch")).alias("endtm_utc_epoch"), col("lcl_to_utc_ofst"))
      .groupBy(col("`type`"), col("tz_id"), col("clct_dt_dt"), col("lcl_to_utc_ofst"))

  }

}
case class ntnl_ofset_fr_day1(
  tz_id:           String,
  clct_dt_dt:      String,
  intb_dt_dt:      String,
  sttm_utc:        String,
  endtm_utc:       String,
  sttm_est:        String,
  endtim_est:      String,
  sttm_lcl:        String,
  endtm_lcl:       String,
  sttm_mvd:        String,
  endtm_mvd:       String,
  est_to_utc_ofst: String,
  lcl_to_utc_ofst: String,
  mvd_to_utc_ofst: String,
  lcl_to_est_ofst: String,
  mvd_to_est_ofst: String,
  mvd_to_lcl_ofst: String,
  lcl_to_mvd_ofst: String)

object ntnl_ofset_fr_day1 {
  def load(spark: SparkSession, dataFile: String): Dataset[ntnl_ofset_fr_day1] = {
    import spark.implicits._
    spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "false")
      .option("escape", "\"")
      .option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true)
      .schema(schemaFor[ntnl_ofset_fr_day1].dataType.asInstanceOf[StructType])
      .csv(dataFile)
      .as[ntnl_ofset_fr_day1]
  }

}