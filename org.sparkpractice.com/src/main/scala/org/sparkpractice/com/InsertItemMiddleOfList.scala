package org.sparkpractice.com

object InsertItemMiddleOfList {
  
  
  def main(args: Array[String]): Unit = {
    
    val ip = 11
    val list = List(1,2,3,4,5,6,7,8,9,7,6)
    
    val len = list.length
    
    val midItem = len/2
    
    println(midItem)
    
    var tempList = list.take(midItem)
    var tempList1=list.drop(midItem)
    
     println(tempList)
     
    var t =  tempList:+ip
    
    println(t:::tempList1)
  }
  
  
  
}