package CEIProject.org.ceiproject.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object wordCountReadMeProg {
  
  def main(args: Array[String]): Unit = {
    
    
    val conf = new SparkConf().setAppName("Word Count Program").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    
    
    val file = "file:///Users/sai kumar/Documents/Practice/CEI_Prac/MovieLens Data/MovieLens Data/README"
    
    val rdd = spark.sparkContext.textFile(file)
    
    val splitFile = rdd.flatMap(x => x.split(" ")) 
    
    val mapValue = splitFile.map(x => (x,1))
    
    val resValue = mapValue.reduceByKey(_ + _)
    
    resValue.repartition(5).saveAsTextFile("file:///Users/sai kumar/Documents/Practice/20230129_2")
    
    
    
  }
  
  
}