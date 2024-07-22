package org.sparkpractice.com


//Remove the duplicate elements from the List

object DistinctList {
  
  
  
  def main(args: Array[String]): Unit = {
    
    val list = List(1,1,2,2,3,4,5,6,7,8,9,11)
    
//    1==1 == Do not consider the 2nd list element -- 1
//    2==1 == Consider this element -- 1,2
//    1==2,2==2 == Do not consider the 2nd list element -- 1,2,3
    
    var empty_List:List[Int]=List()
   
   //Control Statements -- for,for yield, foreach,while,do..while
   //Conditional Statments -- If, if else,case
    
    for(x <- list ) {
      
      if(empty_List.contains(x)) {
        
         println("Duplicate Element")
      }else {
        
        empty_List = empty_List :+ x
        
      }
     
    empty_List
      
    }
    
     empty_List.foreach(println)
   
  }
  
  
}