package com.nielsen.spark
import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.{ SparkSession, DataFrame, Row, Dataset }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.types.StructType
import scala.reflect.api.materializeTypeTag
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset

object MadhuSparkSql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local").appName("app name").config("spark.some.config.option", "True").getOrCreate()
    import spark.implicits._

    var tv_dstbn_source_noncoded: Dataset[tv_dstbn_source_noncoded1] = null
    var tv_media_credit: Dataset[tv_media_credit1] = null

    tv_media_credit = tv_media_credit1.load(spark, "./input/tv_media_credit.csv")
    tv_dstbn_source_noncoded = tv_dstbn_source_noncoded1.load(spark, "./input/tv_dstbn_source_noncoded.csv")

    tv_media_credit.show()

    val tv = tv_media_credit.as("tv")
    val dsrc = tv_dstbn_source_noncoded.as("dsrc")

    val result1 = tv_media_credit.as("tv").select((col("tv.dstbn_source_id").alias("dsrc_id")), (col("tv.internal_call_ltrs").alias("intr_cl_ltrs")), (col("tv.effective_start_date").alias("efct_strt_dt")), (col("tv.effective_end_date").alias("efct_end_dt"))).toDF()

    val result2 = tv_dstbn_source_noncoded.as("dsrc").withColumn("dsrc_id", col("dsrc.dstbn_source_id").cast("BigInt"))
      .select((col("dsrc.dstbn_source_id").alias("dsrc_id")), (col("dsrc.internal_call_ltrs").alias("intr_cl_ltrs")), (col("dsrc.effective_start_date").alias("efct_strt_dt")), (col("dsrc.effective_end_date").alias("efct_end_dt")))
    val check = result2.select(col("dsrc_id"))
    val result3 = result1.union(result2)
    val finalR = result3.select(col("dsrc_id"), col("intr_cl_ltrs"), col("efct_strt_dt"), col("efct_end_dt"))

    finalR.show()

  }
}

case class tv_dstbn_source_noncoded1(
  dstbn_source_id:      String,
  internal_call_ltrs:   String,
  effective_start_date: String,
  effective_end_date:   String)

object tv_dstbn_source_noncoded1 {
  def load(spark: SparkSession, dataFile: String): Dataset[tv_dstbn_source_noncoded1] = {
    import spark.implicits._
    spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "false")
      .option("escape", "\"")
      .option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true)
      .schema(schemaFor[tv_dstbn_source_noncoded1].dataType.asInstanceOf[StructType])
      .csv(dataFile)
      .as[tv_dstbn_source_noncoded1]
  }

}
case class tv_media_credit1(
  dstbn_source_id:      String,
  internal_call_ltrs:   String,
  effective_start_date: String,
  effective_end_date:   String)

object tv_media_credit1 {
  def load(spark: SparkSession, dataFile: String): Dataset[tv_media_credit1] = {
    import spark.implicits._
    spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "false")
      .option("escape", "\"")
      .option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true)
      .schema(schemaFor[tv_media_credit1].dataType.asInstanceOf[StructType])
      .csv(dataFile)
      .as[tv_media_credit1]
  }

}
   