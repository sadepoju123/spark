package org.sparkpractice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.ArrayType

object ActiveSessionHoursInfo {
  
  def main(args: Array[String]): Unit = {
    
    
  val conf = new SparkConf().setAppName("ActiveSessionHoursInfo").setMaster("local[*]")
  val spark= SparkSession.builder().config(conf).getOrCreate()
    
val data = List(
Row(1, "login", "2024-07-23 08:00:00"),
Row(1, "logout", "2024-07-23 12:00:00"),
Row(1, "login", "2024-07-23 13:00:00"),
Row(1, "logout", "2024-07-23 17:00:00"),
Row(2, "login", "2024-07-23 09:00:00"),
Row(2, "logout", "2024-07-23 11:00:00"),
Row(2, "login", "2024-07-23 12:00:00"),
Row(2, "logout", "2024-07-23 15:00:00"),
Row(1, "login", "2024-07-24 08:30:00"),
Row(1, "logout", "2024-07-24 12:30:00"),
Row(2, "login", "2024-07-24 09:30:00"),
Row(2, "logout", "2024-07-24 10:30:00"))


val data1 = List(
List(1, "login", "2024-07-23 08:00:00"),
List(1, "logout", "2024-07-23 12:00:00"),
List(1, "login", "2024-07-23 13:00:00"),
List(1, "logout", "2024-07-23 17:00:00"),
List(2, "login", "2024-07-23 09:00:00"),
List(2, "logout", "2024-07-23 11:00:00"),
List(2, "login", "2024-07-23 12:00:00"),
List(2, "logout", "2024-07-23 15:00:00"),
List(1, "login", "2024-07-24 08:30:00"),
List(1, "logout", "2024-07-24 12:30:00"),
List(2, "login", "2024-07-24 09:30:00"),
List(2, "logout", "2024-07-24 10:30:00"))
  


val schema1 = new StructType(Array(StructField("userDetails",ArrayType(ArrayType(StringType)))))
  
 val rdd =  data1.map(x => Row(x:_*))

val schema = new StructType(Array(StructField("id",IntegerType),
                                  StructField("activity",StringType),
                                  StructField("datetime",StringType)))


val df = spark.createDataFrame(spark.sparkContext.parallelize(data1), schema1)
  
  
    
  }
  
  
}