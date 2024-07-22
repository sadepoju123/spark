package org.sparkpractice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RDDOperations {
  

  
def main(args: Array[String]): Unit = {
  
  
  val conf = new SparkConf().setAppName("RDD Operations").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate()
  
  
  val list = List(1,1,2,2,3,4,5,6,7,8,9,11)
  
  //Convert the list in to RDD, by using paralellize method
  
  val rdd = spark.sparkContext.parallelize(list,3)
  
  //view the contents of the list
  rdd.foreach(print)
  
  val filter_rdd = rdd.filter(x => x != 2 && x != 9)
  filter_rdd.foreach(println)
  
  val map_rdd = rdd.map(x => if(x>4) x else x+":: is Less than 4")
  map_rdd.foreach(println)
  
  //Print even number with EVEN  and odd number with ODD
  
  val even_odd_rdd = rdd.map(x => if(x%2==0) (x,"EVEN") else (x,"ODD"))
  
  even_odd_rdd.foreach(println)
  
  val dist_rdd = rdd.distinct()
  
  dist_rdd.foreach(println)
  
  val union_rdd = rdd.union(rdd)
  
  union_rdd.foreach(println)
  
  println("Single RDD Operation TAKE ::::") 
      union_rdd.take(4).foreach(x => print(x))
  
  val cart_rdd = rdd.cartesian(rdd)
  cart_rdd.foreach(println)
  
  println("::::::::::Count of cartesian product:::::::::")
  println(cart_rdd.count())
  //return first N elements from the list
   println("::::::::::First N elements from the list:::::::::")
  cart_rdd.take(10).foreach(println)
  println("First element from the list::::" + cart_rdd.first())
  println("Number of Partitions :::" + cart_rdd.getNumPartitions)
  
  val rdd2 = spark.sparkContext.parallelize(List("RAJESH","Suresh","Mahesh","raju","Nishanth","RAJESH1","Suresh1","Mahesh1","raju1","Nishanth1","raju2","Nishanth2"),3)
  
  val opRDD = rdd.zip(rdd2)
  
  
  opRDD.foreach(println)
  rdd2.zipWithIndex().foreach(println)
 //cart_rdd.coalesce(5)
   //      .saveAsTextFile("file:///C:/Users/sai kumar/Documents/Practice/BigData_Session_Prac/cartecian_Prod_data_2.txt")
         


}
  
}