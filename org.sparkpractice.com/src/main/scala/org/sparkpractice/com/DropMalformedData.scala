package org.sparkpractice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.col



object DropMalformedData {
  
  def main(args: Array[String]): Unit = {
    
          //Creating Spark Config object
    val conf = new SparkConf().setAppName("Word Count Program").setMaster("local[*]")
    
    //Creating Spark Session Object
    val spark = SparkSession.builder().config(conf).getOrCreate()
   
     spark.conf.set("spark.sql.csv.parser.columnPruning.enabled","false")
    
     //Defining the schema of the data
    
    val schema = StructType(Array(
                                  StructField("PRO_ID",IntegerType),
                                  StructField("PRO_NAME",StringType),
                                  StructField("PRO_PRICE",IntegerType),
                                  StructField("PRO_COM",IntegerType)
                                  ))
                             
        val schema1 = StructType(Array(
                                      StructField("PRO_ID",IntegerType),
                                      StructField("PRO_NAME",StringType),
                                      StructField("PRO_PRICE",IntegerType),
                                      StructField("PRO_COM",IntegerType),
                                      StructField("_corrupt_record",StringType)
                                      ))
        
    val csvFile = spark.read
                       .format("csv")
                       .schema(schema)
                       .option("delimiter",",")
                       .option("mode","dropmalformed")
                       .option("header","true")
                       .option("inferSchema","true")
                      // .option("columnNameOfCorruptRecord","_corrupt_record")
                       .load("file:///C:/Users/sai kumar/Documents/Practice/srcfiles/products_17062024.txt")
                       
     csvFile.show(false)
    
      val csvFile1 = spark.read
                       .format("csv")
                       .schema(schema1)
                       .option("delimiter",",")
                       .option("mode","permissive")
                       .option("header","true")
                       .option("inferSchema","true")
                       .option("columnNameOfCorruptRecord","_corrupt_record")
                       .load("file:///C:/Users/sai kumar/Documents/Practice/srcfiles/products_17062024.txt")
    
     csvFile1.filter(col("_corrupt_record").isNull).show(false)

  }

  
}