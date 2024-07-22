package org.sparkpractice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType


object MultipleDelemiterUseCase {
  
  def main(args: Array[String]): Unit = {
    
        
   //Creating Spark Config object
    val conf = new SparkConf().setAppName("Multiple Delimeter Use Case").setMaster("local[*]")
    
    //Creating Spark Session Object
    val spark = SparkSession.builder().config(conf).getOrCreate()
   
    val csvFile = spark.read
                       .format("csv")
                       .option("delimiter","|")
                       .option("header","true")
                       .option("inferSchema","true")
                      // .option("columnNameOfCorruptRecord","_corrupt_record")
                       .load("file:///C:/Users/sai kumar/Documents/Practice/srcfiles/products_17062024.txt")
                       
    
    var df1 = csvFile.withColumn("PRO_NAME", split(csvFile.col("PRO_NAME$PRO_PRICE"),"\\$" )(0))
    
     df1 = df1.withColumn("PRO_PRICE", split(csvFile.col("PRO_NAME$PRO_PRICE"),"\\$" )(1).cast(IntegerType))
     df1 = df1.drop(col("PRO_NAME$PRO_PRICE"))
     
     df1.printSchema()
     df1.show(false)
    
    // Mother Board$3200  ==> List("Mother Board","3200")
    
     
     
  }
  
}