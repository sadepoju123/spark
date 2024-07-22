package org.sparkpractice.com

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class StudentDataProcessingClass extends studentTrait{
  
  
    //correction in column names 
    //ethnic.group,	english.grade,math.grade,sciences.grade,language.grade,portfolio.rating,
    //coverletter.rating,refletter.rating
  def CorrectionColumnNames(df:DataFrame,spark:SparkSession):DataFrame = {
    
    var df1 = df
    
    df1 = df1.withColumnRenamed("ethnic.group", "ethnic_group")
             .withColumnRenamed("english.grade", "english_grade")
             .withColumnRenamed("math.grade", "math_grade")
             .withColumnRenamed("sciences.grade", "sciences_grade")
             .withColumnRenamed("language.grade", "language_grade")
             .withColumnRenamed("portfolio.rating", "portfolio_rating")
             .withColumnRenamed("coverletter.rating", "coverletter_rating")
             .withColumnRenamed("refletter.rating", "refletter_rating")
             .drop("ethnic.group","english.grade","math.grade","sciences.grade","language.grade","portfolio.rating","coverletter.rating","refletter.rating")
    
    df1

  }
  
//Find the number of students from each country
  
  def CountOfStudentsFromEachCountry(df:DataFrame,spark:SparkSession):DataFrame = {
    
    var df1 = df
//    
//    df1 = df1.groupBy("nationality").agg(count("nationality").alias("CntOfStudentsFromEachCountry"))
//             .select(col("nationality").alias("country"), col("CntOfStudentsFromEachCountry"))
//    
//    df1
    df1.createTempView("student_df_vw")
    
    var df2 = spark.sql("SELECT nationality as country,count(*) as CntOfStudentsFromEachCountry FROM student_df_vw GROUP BY nationality ORDER BY COUNT(*) desc")

     df2

  }
  
//  //Find the student who got highest grade in each subject and identify his country.
  def HighestGradeInEachSubject(df:DataFrame,spark:SparkSession):DataFrame = {
    
    var df1 = df
    
   var engGrade = df1.where(col("english_grade")=== 4).select(col("id"),col("name"),col("nationality"),col("english_grade").alias("grade"),lit("English").alias("Subject"))
   
   var mathGrade = df1.where(col("math_grade") === 4).select(col("id"),col("name"),col("nationality"),col("math_grade").alias("grade"),lit("Maths").alias("Subject"))
   var sciGrade = df1.where(col("sciences_grade") === 4).select(col("id"),col("name"),col("nationality"),col("sciences_grade").alias("grade"),lit("Science").alias("Subject"))
  
   val unionDF = engGrade.union(mathGrade).union(sciGrade)
   
  unionDF
 
  }
  
// //Who got highest grade(maths+ science+english+language)/4 from each country
  
  def HighestGradeFromEachCountry(df:DataFrame,spark:SparkSession):DataFrame = {
    
    var df1 = df
    
    df1 = df1.withColumn("total_grade", (col("english_grade")+col("math_grade")+col("sciences_grade")+col("language_grade"))/4)
             //.select(col("total_grade"), col("id"),col("name"),col("nationality")).orderBy(col("total_grade").desc)
   // df1 = df1.groupBy(col("nationality")).agg(max(col("total_grade")).alias("max_grade")).orderBy(col("max_grade").desc)
    
    val win = Window.partitionBy(col("nationality")).orderBy(col("total_grade").desc)
    val win1 = Window.partitionBy(col("nationality"))
    
    
    df1 = df1.withColumn("max_Grade", max(col("total_grade")).over(win))
             .withColumn("min_grade", min(col("total_grade")).over(win1))
             .withColumn("avg_grade", avg(col("total_grade")).over(win1))
             
             
    df1 = df1.withColumn("rank", rank().over(win))
             .withColumn("dense_rank", dense_rank().over(win))
             .withColumn("row_number", row_number().over(win))
             .withColumn("percent_grade", percent_rank().over(win))
   
    df1 

  }
  
  
  
   def EmployeeDataProcessing(df:DataFrame,spark:SparkSession):DataFrame ={
     
     val win = Window.partitionBy(col("DEPARTMENT_ID")).orderBy(col("salary").asc)
     
     val deDup = df.dropDuplicates("EMPLOYEE_ID")
  
     val empDF = deDup.withColumn("running_sum", sum(col("salary")).over(win))
                      .withColumn("lead_val", lead(col("salary"), 1).over(win))
                      .withColumn("lag_val", lag(col("salary"), 1).over(win))
                      .withColumn("first_val", first(col("salary")).over(win))
                      .withColumn("last_val", last(col("salary")).over(win))

     empDF

   }

  def CountOfStudentsBasedOnGender(df:DataFrame,spark:SparkSession):DataFrame = {
    
   val cnt = df.groupBy(col("gender")).agg(count(col("gender")).alias("count_based_on_gender"))
               .select(col("gender"), col("count_based_on_gender"))
    
    cnt
  }
  

  
}