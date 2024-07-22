package org.sparkpractice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RDDStringOperations {
  
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("RDD String Operations").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
  
    val list = List("Maths","Physics","Computers","Statistics","Telugu","English")
    println(list.length)
    println(list.reverse)
    val rdd = spark.sparkContext.parallelize(list,1)
    
        rdd.map(x => x.toUpperCase()).foreach(println)
        rdd.map(x => x.toLowerCase()).foreach(println)
        rdd.map(x => (x,x.length())).foreach(println)
        rdd.map(x => (x,x.contains('m'))).foreach(println)
        rdd.map(x => x.replace("Stat", "Math")).foreach(println)
        rdd.map(x => x.substring(0,1)).foreach(println)
        
 
  }
}