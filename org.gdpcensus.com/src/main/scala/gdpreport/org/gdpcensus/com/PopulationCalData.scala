package gdpreport.org.gdpcensus.com

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.catalyst.expressions.Substring
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.length
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.max

class PopulationCalData extends CenTrait{
  
  
  def countryGDP(ip_file1:String,ip_file2:String,spark:SparkSession):DataFrame={
    
    println("++++++++++++++INSIDE countryGDP METHOD+++++++++++++++++++++")
    
    val countryDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
                    .load(ip_file1)
    
    val stGDPDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
                    .load(ip_file2)

    
    val exDF= countryDF.withColumn("states", explode(split((col("states").substr(lit(2), length(col("states"))-2)),",")))
    
    val joinDF = exDF.join(stGDPDF,exDF("states")===stGDPDF("state"),"inner").drop(stGDPDF("state"))
       
    
   joinDF
  }
  
  
  def highestAndLowestPopulation(df:DataFrame) {
    
    println("******INSIDE highestAndLowestPopulation METHOD*********** ")
    
    val win = Window.partitionBy("country").orderBy(col("population").asc)
    
    val rnkDF = df.withColumn("popRank", rank().over(win))
    
    val cntIndia = rnkDF.select(max(col("popRank"))).toString().toInt
    
    val ranDF = rnkDF.withColumn("pop_grade",when(col("country")==="india" && col("popRank")===cntIndia,"Highest")
                     .when(col("country")==="india" && col("popRank")===1,"Lowest"))
    
    rnkDF.show(50,false)
  }
  
  def gdpPerformance() {
    println("******INSIDE gdpPerformance METHOD*********** ")
  }
  
  def highestAndLowestGDPInTheCuntry(){
    
     println("******INSIDE highestAndLowestGDPInTheCuntry METHOD*********** ")
  }
  
  def censusReport(){
    
    println("******INSIDE censusReport METHOD*********** ")
  }
  
  
}