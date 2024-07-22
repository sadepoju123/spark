package com.sparkProj.org

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object sparkSumOfTwoNum {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setMaster("local[*]").setAppName("Sum Of Two Numbers")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    
    //collection == List, Array,Map,Set,Option,Seq
    val a=100
    val b=200
    
    var list= List(100,200)
   
    //Create RDD from above list
    
    var rdd1 = spark.sparkContext.parallelize(list)
    
    var op1 = rdd1.take(2).tail
    var op3 = op1.toString().toInt
    
    println(op3)
    
    var op2 = rdd1.take(1).toString().toInt
    
    println(op2)
    
    if(op3==op2) {
      
      var res = 3* (op3+op2)
      println(res)
    }else {
      
      var res = op3+op2
      println(res)
    }
    
    
   
    
    
    
    
  }
}