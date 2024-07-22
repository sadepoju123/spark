package org.sparkpractice.com

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.lit
import java.util.Properties
//import org.apache.spark.sql.functions.col 
//import org.apache.spark.sql.functions.split
//import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.functions._

object DataFileLoad {
  //Check whether the given path is existed or not , if the folder not existed then exit from the job
  //If the folder exists then load only the files data not the sub folders data
  //Save entire data to a file system
  
  //1. check the given path
  //if(path.exists){} else {
  // check whether there are any sub folders , skip those folders
  //if(isFolder==true){
  //}else{
  //}
//}
def main(args: Array[String]): Unit = {
  
  
  val conf = new SparkConf().setAppName("Data File Load").setMaster("local[*]")
  val spark= SparkSession.builder().config(conf).getOrCreate()
  
  
  
  val srcPath = "C:/Users/sai kumar/Documents/Practice/srcfiles/TN_DistData"
  val tgtPath = "C:/Users/sai kumar/Documents/Practice/srcfiles/Output"
  
  var emptyDF = spark.emptyDataFrame
//  val configuration = new Configuration()
//  val fileSystem = FileSystem.get(configuration)
//  val path = new Path(srcPath)
   val path = new File(srcPath)
  
  //Postgresql connection properties
  
  val hostName = "jdbc:postgresql://localhost:5432/raw"
  val userName = "postgres"
  val passWord = "admin"
  
  val conn = new Properties()
            conn.setProperty("url", hostName)
            conn.setProperty("Driver", "org.postgresql.Driver")
            conn.setProperty("user", userName)
            conn.setProperty("password",passWord)
            val tbl_nm = "telangana_dist_census"
  
//  val df1 = spark.read.jdbc(hostName, tbl_nm, conn)
//  
//  df1.select(col("*")).show(false)
 // val query = "(DROP TABLE raw.telangana_dist_census) as t"
//  val query = "(SELECT COUNT(*) FROM raw.telangana_dist_census) as t"
// val cnt =  spark.read.jdbc(hostName, query, conn)
// 
  //   println("***************TABLE HAS BEEN DELETED********************")       
            
  path.listFiles().toList.foreach(file => {
       
  // SELECT "SOME VALUE" AS NEW_COLUMN, salary from employee;
    
    
 if(file.isFile()) {
     println((file.getName,file.length()/(1024)))
       println("NAME OF THE FILE IS:::::"+file.getName)
       var fileNm = file.getName
       var fileNm1 = file.getName.split('P')(0).trim()
       var srcFile = spark.read
                          .format("csv")
                          .option("header", "true")
                          .option("inferSchema", "true")
                          .option("mode","DROPMALFORMED")
                          .load(srcPath +"/"+file.getName )
       
        srcFile = srcFile.withColumn("fileName",trim(split(lit(fileNm).substr(0,fileNm.length()-4),"PCA")(0)) ) 
        srcFile = srcFile.withColumn("fileLenBT",length(split(lit(fileNm).substr(0,fileNm.length()-4),"PCA")(0))) 
        srcFile = srcFile.withColumn("fileLenAT",length(trim(split(lit(fileNm).substr(0,fileNm.length()-4),"PCA")(0))) ) 
       //srcFile.write.mode("append").format("csv").save(tgtPath)   
             
      srcFile.write.mode("append").jdbc(hostName, tbl_nm, conn)

     }else {
       
       println("NAME OF THE FOLDER IS******"+file.getName)
       
     }

   })


}
  

  
}