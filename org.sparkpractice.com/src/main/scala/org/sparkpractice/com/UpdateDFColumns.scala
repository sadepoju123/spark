package org.sparkpractice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.io.File


object UpdateDFColumns {
  
  def main(args: Array[String]): Unit = {
    
     
  val conf = new SparkConf().setAppName("Data File Load").setMaster("local[*]")
  val spark= SparkSession.builder().config(conf).getOrCreate()
  
  
  
  val srcPath = "C:/Users/sai kumar/Documents/Practice/srcfiles/TN_DistData"
  val tgtPath = "C:/Users/sai kumar/Documents/Practice/srcfiles/Output" 
    
  val path = new File(srcPath)
    
  }
  
  
}