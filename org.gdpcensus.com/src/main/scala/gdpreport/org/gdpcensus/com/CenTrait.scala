package gdpreport.org.gdpcensus.com

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

trait CenTrait {
  
  //COUNTRYS WITH STATES AND THIER GDP
  
  def countryGDP(file1:String,file2:String,spark:SparkSession):DataFrame
  
  //Highest and Lowest Population states with in the country
  
  def highestAndLowestPopulation(ip:DataFrame)
  
  def gdpPerformance()
  
  def highestAndLowestGDPInTheCuntry()
  
  def censusReport()
  
  
}