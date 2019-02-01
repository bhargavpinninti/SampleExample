package com.nielsen.spark

import org.apache.log4j.{Level,Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{ SparkSession, DataFrame, Row,Dataset }
import org.apache.spark.sql.functions.col


import org.apache.spark.sql.types.{ StructType, StructField, StringType }


object joinWithmap extends Logging {
  Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local").appName("app name").getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val actuals_date_id = 20181023
    val forecast_date_id = 20180921
    val rowsRdd = sc.parallelize(
      Array(
        Row("Actual", null, "Final", null, s"$actuals_date_id", s"$forecast_date_id"),
        Row("Actual", null, "Final", null, s"$actuals_date_id", s"$forecast_date_id")))
    val schema = StructType(
      StructField("col1", StringType, true) ::
        StructField("col2", StringType, true) ::
        StructField("col3", StringType, true) ::
        StructField("col4", StringType, true) ::
        StructField("col5", StringType, true) ::
        StructField("col6", StringType, true) :: Nil)

    val df  = spark.createDataFrame(rowsRdd, schema)//.map(x=>(x.get(0),x.get(1),x.get(2),x.get(3),x.get(4),x.get(5)))
    val df1 = spark.createDataFrame(rowsRdd, schema)//.map(x=>(x.get(0),x.get(1),x.get(2),x.get(3),x.get(4),x.get(5)))
  
    val joined_df = df.join( df1, df.col("col1") === df1.col("col1"), "LeftOuter").select(df.col("*"))
    //val joined_df  =df.join(df1)

val finaldf = joined_df.map(r=>(test(col1 = df.col("col1").toString())))
  /*  ,col2 = df.col("col2").toString()
    ,col3 = df.col("col3").toString()
    ,col4 = df.col("col4").toString()
    ,col5 = df.col("col5").toString()
    ,col6 = df.col("col6").toString())))*/
        
   finaldf.show(false)
   
  // joined_df.show()
   /*val csds = joined_df.as[test]
        csds.show(false)
   val finalds = csds.map(i => (i.col1))
    finalds.show(false)
   */
      //myMap.get(myKey).map(_.valueParam).getOrElse(defaultParam)
   /* val primitiveDS = Seq(1,2,3).toDS()
val augmentedDS = primitiveDS.map(i => ("var_" + i.toString, (i + 1).toLong)).
 withColumnRenamed ("_1", "name" ).
 withColumnRenamed ("_2", "age" )
augmentedDS.as[Person].show()*/
       /*val finaldf = joined_df.map(r=>
      (df.col("col1") = r.get(0).toString())*/
          //val finaldf = test.map(r=>(df.col_("col1") = r.get(0).toString()))
     // val finaldf1 = joined_df.map(r=> r)
                                      
  }
  
}


 //case class test(col1: String,col2: String,col3: String,col4: String,col5: String,col6: String)

 case class test(col1: String)

