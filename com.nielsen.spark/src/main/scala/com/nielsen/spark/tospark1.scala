package com.nielsen.spark
import org.apache.spark._
import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.plans.LeftOuter

object toSpark1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local").appName("app name").config("spark.some.config.option", "True").getOrCreate()
    import spark.implicits._
    val dftb1 = spark.read.json("T_GL_CC_HIERARCHY_STG2")
    val dftb2 = spark.read.json("%s.t_capex_actuals")
    dftb1.show(10, false)
    dftb2.show(10, false)

    val columnList = Seq("RN", "LEVEL_NUM", "FLEX_VALUE_SET_ID", "PARENT_FLEX_VALUE", " RANGE_ATTRIBUTE", "CHILD_FLEX_VALUE_HIGH", "CHILD_FLEX_VALUE_LOW")

    val L0 = dftb1.filter((dftb1.col("FLEX_VALUE_SET_ID") === ("1003664")) && (dftb1.col("PARENT_FLEX_VALUE")))
      .selectExpr(columnList: _*)

    val L1 = dftb2.filter((dftb1.col("FLEX_VALUE_SET_ID") === "1003664"))
      .selectExpr(columnList: _*)

    val L2 = dftb2.filter((dftb1.col("FLEX_VALUE_SET_ID") === "1003664"))
      .selectExpr(columnList: _*)

    val L3 = dftb2.filter((dftb1.col("FLEX_VALUE_SET_ID") === "1003664"))
      .selectExpr(columnList: _*)

    val L4 = dftb2.filter((dftb1.col("FLEX_VALUE_SET_ID") === "1003664"))
      .selectExpr(columnList: _*)

    val L5 = dftb2.filter((dftb1.col("FLEX_VALUE_SET_ID") === "1003664"))
      .selectExpr(columnList: _*)

    val L6 = dftb2.filter((dftb1.col("FLEX_VALUE_SET_ID") === "1003664"))
      .selectExpr(columnList: _*)

    val L7 = dftb2.filter((dftb1.col("FLEX_VALUE_SET_ID") === "1003664"))
      .selectExpr(columnList: _*)

    val L8 = dftb2.filter((dftb1.col("FLEX_VALUE_SET_ID") === "1003664"))
      .selectExpr(columnList: _*)

    val L9 = dftb2.filter((dftb1.col("FLEX_VALUE_SET_ID") === "1003664"))
      .selectExpr(columnList: _*)

    val L10 = dftb2.filter((dftb1.col("FLEX_VALUE_SET_ID") === "1003664"))
      .selectExpr(columnList: _*)

    val allJoin = (L0.joinWith(L1, L0.col("CHILD_FLEX_VALUE_HIGH") === L1.col("CHILD_FLEX_VALUE_HIGH")
      && (L0.col("RN") - 1 === L1.col("RN"))
      && (L0.col("Level_Num") - 1 === L1.col("LEVEL_NUM")), "LeftOuter")
      .joinWith(L2, L1.col("CHILD_FLEX_VALUE_HIGH") === L2.col("CHILD_FLEX_VALUE_HIGH")
        && (L1.col("RN") - 1) === L2.col("RN")
        && (L1.col("Level_Num") - 1 === L2.col("LEVEL_NUM")), "LeftOuter")
      .joinWith(L3, L2.col("CHILD_FLEX_VALUE_HIGH") === L3.col("CHILD_FLEX_VALUE_HIGH")
        && (L2.col("RN") - 1) === L3.col("RN")
        && (L2.col("Level_Num") - 1 === L3.col("LEVEL_NUM")), "LeftOuter")
      .joinWith(L4, L3.col("CHILD_FLEX_VALUE_HIGH") === L4.col("CHILD_FLEX_VALUE_HIGH")
        && (L3.col("RN") - 1) === L4.col("RN")
        && (L3.col("Level_Num") - 1 === L4.col("LEVEL_NUM")), "LeftOuter")
      .joinWith(L5, L4.col("CHILD_FLEX_VALUE_HIGH") === L5.col("CHILD_FLEX_VALUE_HIGH")
        && (L4.col("RN") - 1) === L5.col("RN")
        && (L4.col("Level_Num") - 1 === L5.col("LEVEL_NUM")), "LeftOuter")
      .joinWith(L6, L5.col("CHILD_FLEX_VALUE_HIGH") === L6.col("CHILD_FLEX_VALUE_HIGH")
        && (L5.col("RN") - 1) === L6.col("RN")
        && (L5.col("Level_Num") - 1 === L6.col("LEVEL_NUM")), "LeftOuter")
      .joinWith(L7, L6.col("CHILD_FLEX_VALUE_HIGH") === L7.col("CHILD_FLEX_VALUE_HIGH")
        && (L6.col("RN") - 1) === L7.col("RN")
        && (L6.col("Level_Num") - 1 === L7.col("LEVEL_NUM")), "LeftOuter")
      .joinWith(L8, L7.col("CHILD_FLEX_VALUE_HIGH") === L8.col("CHILD_FLEX_VALUE_HIGH")
        && (L7.col("RN") - 1) === L8.col("RN")
        && (L7.col("Level_Num") - 1 === L8.col("LEVEL_NUM")), "LeftOuter")
      .joinWith(L9, L8.col("CHILD_FLEX_VALUE_HIGH") === L9.col("CHILD_FLEX_VALUE_HIGH")
        && (L8.col("RN") - 1) === L9.col("RN")
        && (L8.col("Level_Num") - 1 === L9.col("LEVEL_NUM")), "LeftOuter")
      .joinWith(L10, L9.col("CHILD_FLEX_VALUE_HIGH") === L10.col("CHILD_FLEX_VALUE_HIGH")
        && (L9.col("RN") - 1) === L10.col("RN")
        && (L9.col("Level_Num") - 1 === L10.col("LEVEL_NUM")), "LeftOuter"))
      .select(
        L0.col("*"),
        L0.col("Parent_flex_value HIER0_CODE"),
        L1.col("Parent_flex_value HIER1_CODE"),
        L2.col("Parent_flex_value HIER2_CODE"),
        L3.col("Parent_flex_value HIER3_CODE"),
        L4.col("Parent_flex_value HIER4_CODE"),
        L5.col("Parent_flex_value HIER5_CODE"),
        L6.col("Parent_flex_value HIER6_CODE"),
        L7.col("Parent_flex_value HIER7_CODE"),
        L8.col("Parent_flex_value HIER8_CODE"),
        L9.col("Parent_flex_value HIER9_CODE"),
        L10.col("Parent_flex_value HIER10_CODE"))
  }
}