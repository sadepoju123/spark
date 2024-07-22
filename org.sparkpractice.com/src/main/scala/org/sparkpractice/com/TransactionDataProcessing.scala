package org.sparkpractice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.negate
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.lit
import java.util.Properties
import org.apache.spark.sql.functions.current_timestamp



object TransactionDataProcessing {
  
  def main(args: Array[String]): Unit = {
    
     
  val conf = new SparkConf().setAppName("Data File Load").setMaster("local[*]")
  val spark= SparkSession.builder().config(conf).getOrCreate()
  
  spark.sparkContext.setLogLevel("ERROR")
  
  val hostName = "jdbc:postgresql://localhost:5432/raw"
  val hostName_trans = "jdbc:postgresql://localhost:5432/transformed"
  val userName = "postgres"
  val passWord = "admin"
  
  val conn = new Properties()
      conn.setProperty("url", hostName)
      conn.setProperty("Driver", "org.postgresql.Driver")
      conn.setProperty("user", userName)
      conn.setProperty("password",passWord)
  
  val trans_tbl = "transaction_tbl"
  val cust_tbl = "customer_tbl"
  val custHistoryTbl = "customer_history_tbl"
  
  val trans_data = "C:/Users/sai kumar/Documents/Practice/srcfiles/trans_table.txt"
	val cust_data = "C:/Users/sai kumar/Documents/Practice/srcfiles/customer_data.txt"
  
  val trans_data_csv = spark.sparkContext.textFile(trans_data,1)
  val cust_data_csv = spark.sparkContext.textFile(cust_data,1)
  
//  val filterData = trans_data_csv.zipWithIndex().filter(x => x._2>0).map(x => x._1)
//  
//  val filterDataCol = trans_data_csv.zipWithIndex()
//                                    .filter(x => x._2==0)
//                                    .map(x => x._1.split(" "))
//  filterDataCol.collect().foreach(println)
  

  
  val split_data = trans_data_csv.map(x => x.split(" ")).map(x => Row(x:_*))
  
  val split_data1 = cust_data_csv.map(x => x.split(" ")).map(x => Row(x:_*))
  
 // customer_id,transaction_type,transaction_amount
  
  val schema = new StructType(Array(StructField("customer_id",StringType),
                                    StructField("transaction_type",StringType),
                                    StructField("transaction_amount",StringType)))
  
  val schema1 = new StructType(Array(StructField("customer_id",StringType),
                                    StructField("current_amount",StringType)
                                    ))
  var trans_df = spark.createDataFrame(split_data, schema)
  
  
  var cust_df = spark.createDataFrame(split_data1, schema1)
  
  trans_df = trans_df.withColumn("num", monotonically_increasing_id())
  cust_df = cust_df.withColumn("num", monotonically_increasing_id())
  
  trans_df = trans_df.where(col("num") >=1).drop(col("num"))
                     .withColumn("last_updated_ts",current_timestamp())
                     .select(col("customer_id").cast("int"), col("transaction_type"),col("transaction_amount").cast("int"),col("last_updated_ts"))
  
  cust_df = cust_df.where(col("num") >=1).drop(col("num"))
                   .withColumn("last_updated_ts",current_timestamp())
                   .select(col("customer_id").cast("int"),col("current_amount").cast("int"),col("last_updated_ts"))

                   
  println("Load Data to History Table")
  
  var readCustData = spark.read.jdbc(hostName, cust_tbl, conn)
  
  readCustData = readCustData.withColumn("update_ts", current_timestamp())
  readCustData.write.mode("append").jdbc(hostName, custHistoryTbl, conn)
  
  
  
  println("Writing the data to PostgreSQL")
  
  trans_df.write.mode("append").jdbc(hostName, trans_tbl, conn)
  cust_df.write.mode("append").jdbc(hostName, cust_tbl, conn)
   
  var trans_tbl_df = spark.read.jdbc(hostName, trans_tbl, conn)
  var cust_tbl_df = spark.read.jdbc(hostName, cust_tbl, conn)
  println("+++++++++++++++Transaction Table Data+++++++++++++++++")
  trans_tbl_df.show()
  println("+++++++++++++++Customer Table Data+++++++++++++++++")
  cust_tbl_df.show()                    
 
  
  trans_tbl_df = trans_tbl_df
              .withColumn("new_trans_amt", when(col("transaction_type").isin("credit"),col("transaction_amount")).otherwise(negate(col("transaction_amount"))))             
              .drop(col("transaction_amount"))
  
  trans_tbl_df.createTempView("trans_df_vw")           
  
  var trans_tbl_df_vw = spark.sql(s"select customer_id,sum(new_trans_amt) as bal_amt from trans_df_vw group by customer_id ")
  
  
  var joinDF = trans_tbl_df_vw.alias("r").join(cust_tbl_df.alias("t"), cust_tbl_df("customer_id")===trans_tbl_df_vw("customer_id"), "right")
                          .select(cust_tbl_df("customer_id"),trans_tbl_df_vw("bal_amt"),cust_tbl_df("current_amount"))
                          .withColumn("new_bal_amt", when(col("bal_amt").isNull,lit(0)).otherwise(col("bal_amt")))
                          .withColumn("updated_ts",current_timestamp())
                          .drop(col("bal_amt"))
                          .select(cust_tbl_df("customer_id"),(col("new_bal_amt")+cust_tbl_df("current_amount")).alias("Outstanding_amount"),col("updated_ts"))
                         
                        
  val data_trans_tbl = "customer_data"
  
  joinDF.write.mode("append").jdbc(hostName_trans, data_trans_tbl, conn)
//  trans_df.printSchema()
//  
//  trans_df.show()
//  
//  cust_df.printSchema()
//  
//  cust_df.show()
// 
  
  
  }
  
}