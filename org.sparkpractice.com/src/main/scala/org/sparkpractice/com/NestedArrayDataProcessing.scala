package org.sparkpractice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions.flatten

object NestedArrayDataProcessing extends App{
  
  val data = List(
      Row("James",List(List("spark","scala"),List("spark","java"))),
                 Row("John",List(List("java","scala"),List("spark","java"))),
                 Row("Samson",List(List("spark","spark","java")))
                  )
  
  val conf = new  SparkConf().setAppName("Nested Data Processing").setMaster("local[*]")
  val spark= SparkSession.builder().config(conf).getOrCreate()
  
  //val rdd = spark.sparkContext.parallelize(data)
  
  
  val schema = new StructType(Array(StructField("name",StringType),
                                    StructField("Course",ArrayType(ArrayType(StringType)))))
  
  
  
  val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
  
  var df1 = df.withColumn("new_crs", flatten(col("Course")))
  
   //df1 = df1.withColumn("crs", flatten(col("new_crs")))
  
//   df1 = df1.groupBy("name", "crs").agg(count(col("crs")).alias("cnt"))
//   df1 = df1.withColumn("cnt_crs", concat_ws(":",col("crs"),col("cnt")))
    
  // df1 = df1.groupBy("name").agg(collect_list(col("cnt_crs")))
 
  df1.show(false)
  
  //rdd.foreach(println)
}