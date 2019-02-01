package com.nielsen.spark

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import scala.collection.JavaConversions._

object Decompose {
  
  val query = f"""
    select * from ( SELECT a.intab_period_id, a.aggregation_type_code, a.brand_id, b.brand_name, a.sub_brand_id, sb.sub_brand_name, a.content_type, ct.content_type_desc, a.platform_type_code, p.device_platform_name AS platform, a.device_type_code, d.device_type_desc AS device, AVG(a.unique_audience) over (partition BY a.aggregation_type_code, a.brand_id, a.sub_brand_id, a.content_type,a.platform_type_code, a.device_type_code, a.device_type_code) AS avg_aud_3months, AVG(a.impressions) over (partition BY a.aggregation_type_code, a.brand_id, a.sub_brand_id, a.content_type,a.platform_type_code, a.device_type_code, a.device_type_code) AS avg_imp_3months, AVG(a.timespent) over (partition BY a.aggregation_type_code, a.brand_id,a.sub_brand_id , a.content_type,a.platform_type_code, a.device_type_code,a.device_type_code) AS avg_timespent_3months, AVG(a.app_launch) over (partition BY a.aggregation_type_code, a.brand_id, a.sub_brand_id, a.content_type,a.platform_type_code, a.device_type_code, a.device_type_code) AS avg_applaunches_3months, AVG(a.app_timespent) over (partition BY a.aggregation_type_code, a.brand_id, a.sub_brand_id, a.content_type,a.platform_type_code, a.device_type_code, a.device_type_code) AS avg_app_timespent_3months FROM dcr_summary_fact a JOIN intab_period ip ON a.intab_period_id=ip.intab_period_id AND a.intab_period_type_code=ip.intab_period_type_code LEFT JOIN ( SELECT DISTINCT brand_id, brand_name FROM brand WHERE '2018-06-01' BETWEEN eff_start_date AND eff_end_date ) b ON a.brand_id=b.brand_id LEFT JOIN ( SELECT DISTINCT sub_brand_id, sub_brand_name, brand_id FROM sub_brand WHERE '2018-06-01' BETWEEN eff_start_date AND eff_end_date) sb ON a.brand_id=sb.brand_id AND sb.brand_id=b.brand_id AND a.sub_brand_id=sb.sub_brand_id LEFT JOIN content_type ct ON ct.content_type_code = a.content_type LEFT JOIN platform_type p ON p.platform_type_code = a.platform_type_code LEFT JOIN device_type d ON d.device_type_code = a.device_type_code WHERE a.intab_period_id IN ( SELECT DISTINCT intab_period_id FROM intab_period WHERE intab_period_type_code=131 AND intab_period_end_date BETWEEN to_date(add_months(date_sub (TRUNC(current_timestamp() , 'month'), 1), 4) ) AND to_date(add_months (date_sub(TRUNC(current_timestamp(), 'month'), 1), 2)))  AND a.aggregation_type_code IN (4,10,16) AND a.reportable_demo_id=30000 AND a.country_id=1 )x
    """
  def main(args: Array[String]): Unit = {

    val warehouse_location = "spark-warehouse"
    val spark = sparkSession(warehouse_location)
    spark.conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
    spark.conf.set("fs.s3a.awsAccessKeyId", "AKIAJWKTWC7NF5RMT3BQ");
    spark.conf.set("fs.s3a.awsSecretAccessKey", "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");
    spark.sparkContext.getConf.set("spark.io.compression.codec", "snappy")
    import spark.implicits._

   val  device_type_df = readParqFile(spark,"s3://useast1-nlsn-w-platforms-mdl-digital-encrypted-prod-01/data/dcr/digital_dcr_mch_prod/external/device_type/")
        device_type_df.createOrReplaceTempView("device_type")
        executeSQL(spark, "msck repair table dcr_summary_fact").show(false)
        
   val outputLocation = "/user/hadoop/test_bg/"
    
    val df = executeSQL(spark,query)
    df.printSchema()
    // df.show()
    
    df.write.option("header", "true").mode(SaveMode.Overwrite).csv(outputLocation)
    
    
  }
    // .master("local")
//.enableHiveSupport()
  def sparkSession(warehouse_location: String) : SparkSession = {
    val spark = SparkSession.builder.appName("Python Spark SQL Hive integration example")
   
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouse_location)
      .getOrCreate()

    return spark
  }

  def stop(sparkSession: SparkSession) = {
    sparkSession.stop()
  }

  def executeSQL(sparkSession: SparkSession, query: String):Dataset[Row] = {
    return sparkSession.sql(query)
  }
  def readParqFile(sparkSession: SparkSession, s3location: String) : Dataset[Row] = {
    val df = sparkSession.read.parquet(s3location)
    return df
  }

}