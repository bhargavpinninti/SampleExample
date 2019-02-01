package com.nielsen.spark
import org.apache.spark._
import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.functions._

object toSpark {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local").appName("app name").config("spark.some.config.option", "True").getOrCreate()
    import spark.implicits._
    val dftb1 = spark.read.json("%s.t_capex_forecast_stg")
    val dftb2 = spark.read.json("%s.t_capex_actuals")
    dftb1.show(10, false)
    dftb2.show(10, false)

    val columnList = Seq("account", "account_category", "account_grp1", "account_grp2", "account_grp3", "account_grp4", "account_grp5", "activity_code", "amount", "cost_center", "cost_type", "country", "currency", "date_id", "fiscal_date_id", "fiscal_period", "fiscal_year", "intercompany", "legal_entity", "product", "region", "rtn_number", "scenario", "service", "version", " period")

    val result = dftb1.selectExpr(columnList: _*)

    val result1 = dftb1.filter(dftb1.col("date_id").geq("2018-07-31"))
      .withColumn("period", concat(trim(dftb1.col("fiscal_period")), lit("-"), substring(dftb1.col("fiscal_year"), 3, 2)))
      .withColumn("scenario", col("Outlook"))
      .selectExpr(columnList: _*)

    val result2 = dftb2.filter((dftb2.col("scenario") === "FY18MIDYRFCST") && (dftb2.col("version") === "Final"))
      .withColumn("period", concat(trim(dftb1.col("fiscal_period")), lit("-"), substring(dftb1.col("fiscal_year"), 3, 2)))
      .withColumn("scenario", col("Outlook"))
      .selectExpr(columnList: _*)

    val finalR = result1.union(result2)
    val finalR1 = result1.unionAll(result2)
    finalR.show(10, false)
    val re = Seq(result1,result2).reduce(_ union _)
    re.show(10,false)
    println(re.rdd.map(x=>x.toString().trim()).collect().toList)

  }
}