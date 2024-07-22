package org.sparkpractice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object StudentDataProcessing {
  
//0,1 --- JVM -- JAVA VIRTUAL MACHINE
  //executable program..program initialization
  
  def main(args: Array[String]): Unit = {
    
    
    //Initialize the Spark driver program
    
    val conf = new SparkConf().setAppName("Student Data Processing").setMaster("local[*]")
   
    //create spark session object
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    
        spark.sparkContext.setLogLevel("ERROR")
    //read student data file from the Windows file system
    
    val filePath = "C:/Users/sai kumar/Documents/Practice/srcfiles/studentdataset.csv"
    
    val df = spark.read.format("csv").option("inferSchema","true").option("header","true").load(filePath)
    
    val empData = "C:/Users/sai kumar/Documents/Practice/srcfiles/Employees_data.csv"
    
    val empdf = spark.read.format("csv").option("inferSchema","true").option("header","true").load(empData)
    
    
    val stuData = new StudentDataProcessingClass()
    
   val empWinData =  stuData.EmployeeDataProcessing(empdf,spark)
   
  // empWinData.coalesce(1).write.format("csv").option("header","true").save("C:/Users/sai kumar/Documents/Practice/srcfiles/EmpWinData8")

    //correction in column names 
    //ethnic.group,	english.grade,math.grade,sciences.grade,language.grade,portfolio.rating,
    //coverletter.rating,refletter.rating
    
   val colNameCorr =  stuData.CorrectionColumnNames(df,spark)
    
   //colNameCorr.show()
      
    //Find the number of students from each country
    
    val cntFromCountry = stuData.CountOfStudentsFromEachCountry(colNameCorr,spark)
    
   // cntFromCountry.show(50,false)
   
    //Find the student who got highest grade in each subject and identify his country.
    val HGradeEachSubject = stuData.HighestGradeInEachSubject(colNameCorr,spark)
    
   // HGradeEachSubject.coalesce(1).write.format("csv").option("header","true").save("C:/Users/sai kumar/Documents/Practice/srcfiles/HGradeEachSubject")
  //  HGradeEachSubject.show()
//    //Who got highest grade(maths+ science+english+language)/4 from each country
    
    println("+++++++++++Printing HighestGradeFromEachCountry Data +++++++++++++++++++++")
   val HGradeFromEachCountry = stuData.HighestGradeFromEachCountry(colNameCorr,spark)
   // HGradeFromEachCountry.coalesce(1).write.format("csv").option("header","true").save("C:/Users/sai kumar/Documents/Practice/srcfiles/HGradeEachCountry3")
//    //Find the total grade of each student
//    stuData.TotalGradeOfEachStudent(df,spark)
//    
    //Find the number of students based on gender
    val cntOfStuGender = stuData.CountOfStudentsBasedOnGender(colNameCorr,spark)

    cntOfStuGender.show(false)
  }
  
  
  
  
}