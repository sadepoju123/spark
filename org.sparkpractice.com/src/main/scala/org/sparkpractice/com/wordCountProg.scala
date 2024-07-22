package org.sparkpractice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object wordCountProg {
    
  def main(args: Array[String]): Unit = {
    
    //Creating Spark Config object
    val conf = new SparkConf().setAppName("Word Count Program").setMaster("local[*]")
    
    //Creating Spark Session Object
    val spark = SparkSession.builder().config(conf).getOrCreate()
    
    //Reading the data from the file system
    val rdd = spark.sparkContext.textFile("file:///C:/Users/sai kumar/Desktop/SparkNotes.txt")
    
    println("***********Printing Number of partitions created***************")
    println(rdd.getNumPartitions)
    println("**************************")
    
    //Counting the words
    val word = rdd.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey(_ + _)
    //val word = rdd.flatMap(x => x.split(" ")).map(x => (x,1)).groupByKey()
    //Sorting data based on highly repeated word
    //val sort_data = word.map(x => (x._2,x._1)).sortByKey(false,1)
    
   // val filter_data = sort_data.filter(x => x._2.contains("5"))
   
    //printing the data to the console
    word.foreach(println)
    
    
  }
  
}