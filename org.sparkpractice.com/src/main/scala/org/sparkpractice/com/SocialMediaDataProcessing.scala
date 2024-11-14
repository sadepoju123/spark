package org.sparkpractice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.functions.count

object SocialMediaDataProcessing extends App {
  

  val conf = new SparkConf().setAppName("Social Media Data Processing").setMaster("local[*]")
  
  val spark = SparkSession.builder().config(conf).getOrCreate()
  
    spark.sparkContext.setLogLevel("ERROR")
  val data = spark.sparkContext.textFile("file:///C:/Users/sai kumar/Documents/Practice/srcfiles/SocialMediaData.txt",1)
  
 // val firstRecordSchema = data.take(1).flatMap(x => x.split(",")).map(x => x.trim()).toList
  
 // val data1 = data.flatMap(x => x.split(",")).map(x => Row(x.trim():_*))
  
 // val df = data1
  //total length = n , index position = n-1
 //source, datasize, date
 val rdd = data.zipWithIndex().filter(x => x._2>0).map(x => x._1)
 
 val schema = new StructType(Array(StructField("source",StringType),
                                   StructField("datasize",StringType),
                                   StructField("src_date",StringType)))
  
  
val rdd1 = rdd.map(x => x.split(",")).map(x => Row(x:_*))

var df = spark.createDataFrame(rdd1, schema)

df = df.select(trim(col("source")).alias("source"),trim(col("datasize")).alias("datasize"),trim(col("src_date")).alias("src_date").cast("date"))
df = df.withColumn("new_ds", split(col("datasize"),"G")(0).cast("int"))

val win = Window.partitionBy(col("src_date"))
val winRank = Window.partitionBy(col("src_date")).orderBy(col("new_ds").desc)

df = df.withColumn("max_val", max(col("new_ds")).over(win))
val rankdf = df.withColumn("rank_val", rank().over(winRank))
               .where(col("rank_val")===1)
               .select(col("source").alias("HighestDataSource"), col("new_ds"))

 val joinDF = rankdf.join(broadcast(df), df("max_val")===rankdf("new_ds"))
                    .select(df("source"),df("datasize"),df("src_date"),rankdf("HighestDataSource"))
                   
 joinDF.createTempView("joindf_vw")
 
 val df4 = spark.sql("select * from joindf_vw").groupBy("source", "src_date").agg(count("*"))
 
  df4.collect()          

}