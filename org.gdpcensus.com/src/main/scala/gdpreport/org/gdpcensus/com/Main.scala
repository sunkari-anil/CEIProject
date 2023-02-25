package gdpreport.org.gdpcensus.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object Main {
  
  
  def main(args: Array[String]): Unit = {
    
    //reading the files path
    
    val conf = new SparkConf().setAppName("Census Report").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    
    
    val file1 = "file:///Users/sai kumar/Documents/Practice/country_dataset.csv"
    val file2 = "file:///Users/sai kumar/Documents/Practice/state_gdp.csv"
    
    
    
    val pop = new PopulationCalData()
   val ip1 = pop.countryGDP(file1, file2,spark)
   pop.highestAndLowestPopulation(ip1)
    pop.censusReport()
    pop.gdpPerformance()
    pop.highestAndLowestGDPInTheCuntry()
    
    
    
  }
  
  
  
  
}