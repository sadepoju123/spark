package org.sparkpractice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
//Find the length of the sentence and count the number of words each sentence has.
//Find the Largest word/string in each sentence and their length
object WordCountUseCase {
  
  def main(args: Array[String]): Unit = {
    
        //Creating Spark Config object
    val conf = new SparkConf().setAppName("Word Count Program").setMaster("local[*]")
    
    //Creating Spark Session Object
    val spark = SparkSession.builder().config(conf).getOrCreate()
    
    val inputFile = spark.sparkContext.textFile("file:///C:/Users/sai kumar/Desktop/UseCase1.txt")
    
    val flattenData = inputFile.flatMap(x => x.split("\\."))
    
    val mapData = flattenData.map(x => (x,x.length()) )
    
    val splitData = mapData.map(x => {
      
      var a = x._1.split(" ").length
      var c = x._1.split(" ").map(x => (x,x.length())).sortBy(x => (x._2)).reverse.head
      var b = x._2
      
      (x._1,a,b,c)
            
    })
    
    splitData.foreach(x => println(x))
    
    splitData.saveAsTextFile("file:///C:/Users/sai kumar/Documents/Practice/BigData_Session_Prac/WordCountUseCase.txt")
   //flattenData.
    
  }
  
  
  
}