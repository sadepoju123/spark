package org.sparkpractice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object PairRDDFunctions {
  
  
  def main(args: Array[String]): Unit = {
    
        //Creating Spark Config object
    val conf = new SparkConf().setAppName("Pair RDD Functions").setMaster("local[*]")
    
    //Creating Spark Session Object
    val spark = SparkSession.builder().config(conf).getOrCreate()
    
    
    val studentRDD = spark.sparkContext.parallelize(Array(
    ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91), ("Joseph", "Biology", 82), 
    ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62), ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80), 
    ("Tina", "Maths", 78), ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87), 
    ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91), ("Thomas", "Biology", 74), 
    ("Cory", "Maths", 56), ("Cory", "Physics", 65), ("Cory", "Chemistry", 71), ("Cory", "Biology", 68), 
    ("Jackeline", "Maths", 86), ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83), 
    ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64), ("Juan", "Biology", 60)), 3)
    
   val mapRDDFun = studentRDD.map(x => (x._1,(x._2,x._3)))
   
    mapRDDFun.foreach(println)
    
    
  }
      

  
}