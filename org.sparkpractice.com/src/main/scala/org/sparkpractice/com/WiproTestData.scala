package org.sparkpractice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object WiproTestData extends App{
  
  
  //
  val str = "I am learing Hadoop"
  val list = str.toList
  //prog to find most repeated character
  
val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
val spark = SparkSession.builder().config(conf).getOrCreate()

val rdd = spark.sparkContext.parallelize(list)

val res = rdd.map(x =>(x,1)).reduceByKey(_+_).map(x => Row(x._2,x._1.toString()))

val schema = new StructType(Array(StructField("count",IntegerType),StructField("CharValue",StringType)))
  
  val df = spark.createDataFrame(res, schema)
  //df.show(false)

 //val rowRDD = res.map(x => Row(x:_*))
  
 //val saveDF = df.write.format("parquet").mode("append").saveAsTable("db.tableName")
 
    
    
 
    
  
  
}