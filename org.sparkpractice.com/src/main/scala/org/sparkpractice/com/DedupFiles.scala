package org.sparkpractice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DedupFiles {
  
  def main(args: Array[String]): Unit = {
    
   //Creating Spark Config object
    val conf = new SparkConf().setAppName("Word Count Program").setMaster("local[*]")
    
    //Creating Spark Session Object
    val spark = SparkSession.builder().config(conf).getOrCreate()
    
         
    val csvFile = spark.read
                       .format("csv")
                       .option("delimiter",",")
                       .option("header","true")
                       .option("inferSchema","true")
                       .load("file:///C:/Users/sai kumar/Documents/Practice/srcfiles/products.csv")
                       
   // csvFile.show(false)
    
   //csvFile.distinct().show()
    println("+++++++++++++++++++++++++++++++++++++++++++")
    println("+++++++++++++++++++++++++++++++++++++++++++")
    
    csvFile.dropDuplicates("PRO_ID","PRO_NAME").show(false)
  
  }
  
  
}