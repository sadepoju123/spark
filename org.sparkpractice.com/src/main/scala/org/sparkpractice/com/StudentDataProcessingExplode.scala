package org.sparkpractice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.length
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions.collect_set
import org.apache.spark.sql.functions.array



object StudentDataProcessingExplode extends App {
  
  
  val conf = new SparkConf().setAppName("Student Data Processing").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate()
  
  import spark.implicits._
  
//  var stuData = spark.read.format("csv")
//                          .option("inferSchema", "true")
//                          .option("header", "true")
//                          .load("file:///C:/Users/sai kumar/Documents/Practice/srcfiles/StudentInfo.csv")
//                          
  var teleData = spark.read.format("csv")
                          .option("inferSchema", "true")
                          .option("header", "true")
                          .load("file:///C:/Users/sai kumar/Documents/Practice/srcfiles/TelcomData.csv")
  
   
                          
     teleData = teleData.withColumn("phones_arr", explode(array(col("ph1"),col("ph2"),col("ph3"),col("ph4")))) 
     
     teleData.where(col("phones_arr").isNotNull).select(col("Name"),col("phones_arr")).show(false)

                          
//  val len = stuData.select(length(col("Marks"))).distinct().first().mkString.toInt
//  
//  println(len)
//  
//  stuData = stuData.withColumn("Sub_marks", explode(split(substring(col("Marks"),2,len-2),",") ))                     
//  
//  var revExplodeStuData = stuData
//  
//  revExplodeStuData = revExplodeStuData.groupBy(col("ID"),col("Name"),col("Class")).agg(collect_list("Sub_marks").alias("Array_Marks"))
//  revExplodeStuData.show(false)
//  stuData.show(false)
  
  
}