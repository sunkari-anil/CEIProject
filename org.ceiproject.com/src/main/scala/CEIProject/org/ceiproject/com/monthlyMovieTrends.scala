package CEIProject.org.ceiproject.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.util.Properties

object monthlyMovieTrends {
  
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Monthly Movie Trends").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    
    val url ="jdbc:postgresql://localhost:5432/raw"
    val conn = new Properties()
    conn.setProperty("Driver", "org.postgresql.Driver")
    conn.setProperty("url", "jdbc:postgresql://localhost:5432/raw")   
    conn.setProperty("user", "postgres")
    conn.setProperty("password", "admin")
    
    
    
    val mvRatingDF = spark.read.jdbc(url, "transdata_tbl", conn)
    
    mvRatingDF.show(false)
    
    
    
    
  }
  
}