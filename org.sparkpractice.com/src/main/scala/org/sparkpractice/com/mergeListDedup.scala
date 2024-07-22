package org.sparkpractice.com

object mergeListDedup {
  
  
//Merge list where there is common elements 
//List(List("1", "2"), List("3", "2"), List("4", “6”), List(“3”, “5”))
//=> List(List(4, 6), List(1, 2, 3, 5))
  
  def main(args: Array[String]): Unit = {
    
  
  val list = List(List(1, 2), List(3, 2), List(4, 6), List(3, 5))
  
  var emptyList:List[Int] = List()
  var emptyList1:List[Int] = List()
  var distHD:List[Int] = List()
  var hd = list.head
  var tl = list.tail
  
   
  for(x <- tl){
    
  
   if(hd.contains(x(0)) || hd.contains(x(1))){
     //println("INSIDE IF STMT:::"+ip(0))
     hd = hd:+x(0):+x(1)
    // println("CoMBINED LIST :::" + hd.distinct)
     distHD= hd.distinct
     distHD
     
   }else {
      //println("INSIDE ELSE STMT :::" + ip)
     emptyList1 = x.toList
     
   }
    
  //println (distHD, emptyList1)
  }
   println (List(distHD, emptyList1))
 
 
  }
  
}