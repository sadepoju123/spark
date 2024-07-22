package org.sparkpractice.com

import org.apache.spark.sql._

trait studentTrait {
  
     //correction in column names 
    //ethnic.group,	english.grade,math.grade,sciences.grade,language.grade,portfolio.rating,
    //coverletter.rating,refletter.rating
  def CorrectionColumnNames(df:DataFrame,spark:SparkSession):DataFrame 
        

//Find the number of students from each country
  
  def CountOfStudentsFromEachCountry(df:DataFrame,spark:SparkSession):DataFrame 
  
//  //Find the student who got highest grade in each subject and identify his country.
  def HighestGradeInEachSubject(df:DataFrame,spark:SparkSession):DataFrame 
  
// //Who got highest grade(maths+ science+english+language)/4 from each country
//  
  def HighestGradeFromEachCountry(df:DataFrame,spark:SparkSession):DataFrame 
  
  def EmployeeDataProcessing(df:DataFrame,spark:SparkSession):DataFrame
  
//   //Find the total grade of each student
//  
//  def TotalGradeOfEachStudent(df:DataFrame,spark:SparkSession):DataFrame 
//
//  //Find the number of students based on gender
//  
  def CountOfStudentsBasedOnGender(df:DataFrame,spark:SparkSession):DataFrame
  
  
}