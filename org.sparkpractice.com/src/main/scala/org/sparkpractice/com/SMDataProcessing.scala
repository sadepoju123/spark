package org.sparkpractice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._


object SMDataProcessing extends App {
  val conf = new SparkConf().setAppName("Data File Load").setMaster("local[*]")
  val spark= SparkSession.builder().config(conf).getOrCreate()

  
    spark.sparkContext.setLogLevel("ERROR")
  
  val SM_data = "C:/Users/sai kumar/Documents/Practice/srcfiles/SocialMediaData.txt"
  
  val rdd = spark.sparkContext.textFile(SM_data, 1)
  
  val rowRDD = rdd.zipWithIndex().filter(x => x._2>0).map(x => x._1.split(",")).map(x => Row(x:_*))
  
  val schema = new StructType(Array(StructField("source",StringType),
                                    StructField("datasize",StringType),
                                    StructField("src_date",StringType)))
  var SM_DF = spark.createDataFrame(rowRDD, schema)
  
  SM_DF = SM_DF.
          select(trim(col("source")).alias("source_1"),trim(col("datasize")).alias("datasize_1"),to_date(trim(col("src_date"))).alias("src_date_1"))
  
  val win = Window.partitionBy(col("src_date_1")).orderBy(col("datasize_new").desc)
  val win1 = Window.partitionBy(col("src_date_1")).orderBy(col("datasize_new").desc)
  SM_DF = SM_DF.withColumn("datasize_new", split(col("datasize_1"),"G")(0).cast("int"))
  
  SM_DF.createTempView("sm_data_vw")
  val SM_DF2 = SM_DF.withColumn("max_val", max("datasize_new").over(win))   
  
  val SM_DF1 = SM_DF.withColumn("rank", rank() over(win1))
               .where(col("rank")===1).select(col("source_1"),col("datasize_new"))
  
              
              
  
 // val maxGrpData = SM_DF.groupBy(col("src_date_1"),col("source_1")).agg(max(col("datasize_new")).alias("grp_max_val"))
  
  
 
  val joinDF = SM_DF1.join(SM_DF2,SM_DF2("max_val")===SM_DF1("datasize_new"),"inner")
                     .withColumn("HighestDataSource", SM_DF1("source_1"))
                    
  
  //maxGrpData.show() 
  SM_DF.printSchema()
  SM_DF1.show()
  SM_DF2.show()
  joinDF.show()
  
  
}