package org.sparkpractice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.negate
import java.util.Properties


object CustomerDataProcessing {
  
  def main(args: Array[String]): Unit = {
    
    
  val conf = new SparkConf().setAppName("Data File Load").setMaster("local[*]")
  val spark= SparkSession.builder().config(conf).getOrCreate()
  
  spark.sparkContext.setLogLevel("ERROR")
  
  val hostName = "jdbc:postgresql://localhost:5432/raw"
  val userName = "postgres"
  val passWord = "admin"
  
  val conn = new Properties()
      conn.setProperty("url", hostName)
      conn.setProperty("Driver", "org.postgresql.Driver")
      conn.setProperty("user", userName)
      conn.setProperty("password",passWord)
  
  val cust = spark.sparkContext.textFile("C:/Users/sai kumar/Documents/Practice/srcfiles/customer_data.txt")
  
  
//  val cust_csv = spark.read.format("csv").option("header", false).option("inferSchema",true).load("C:/Users/sai kumar/Documents/Practice/srcfiles/customer_data.txt")
//  cust_csv.show()
  
  
  val trans = spark.sparkContext.textFile("C:/Users/sai kumar/Documents/Practice/srcfiles/trans_table.txt")
  
  val cust_rdd = cust.map(x => x.split(" ")).map(x => Row(x:_*))
  
  val trans_rdd = trans.map(x => x.split(" ")).map(x => Row(x:_*))
  
  val schema = new StructType(Array(StructField("customer_id",StringType),
                                    StructField("current_amount",StringType)))
  
  val trans_schema = new StructType(Array(StructField("customer_id",StringType),
                                          StructField("transaction_type",StringType),
                                          StructField("transaction_amount",StringType)))
  
  var cust_df = spark.createDataFrame(cust_rdd, schema)
  
  cust_df = cust_df.withColumn("index",monotonically_increasing_id()) 
  cust_df = cust_df.where(col("index")>=1).drop(col("index")).select(col("customer_id").cast("Int"),col("current_amount").cast("Int"))
  
  var trans_df = spark.createDataFrame(trans_rdd, trans_schema)
  
  trans_df = trans_df.withColumn("index", monotonically_increasing_id())
  trans_df = trans_df.where(col("index") >= 1).drop(col("index")).select(col("customer_id").cast("Int"),col("transaction_type"),col("transaction_amount").cast("Int"))
//  cust_df.printSchema()
//  trans_df.printSchema()
//  cust_df.show()
//  trans_df.show()
  
  trans_df = trans_df.withColumn("trans_amount", when(col("transaction_type").isin("credit"),col("transaction_amount")).otherwise(negate(col("transaction_amount")))).drop(col("transaction_amount"))
  trans_df = trans_df.toDF
  trans_df.createTempView("trans_df_vw")
  
  val trans_df1 = spark.sql("select customer_id, sum(trans_amount) as trans_sum from trans_df_vw group by customer_id ")
  
  trans_df.show()
  trans_df1.show()
  //trans_rdd.foreach(println)

    
  }
  
  
}