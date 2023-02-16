package CEIProject.org.ceiproject.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.functions.col

object MostPopularAndLeastPopularMovie {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Most Popular And Least Popular Movie").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    
    //reading data from postgressql
    
    val url = "jdbc:postgresql://localhost:5432/raw"
    val userName = "postgres"
    val password="admin"
    val conn = new Properties()
    conn.setProperty("Driver", "org.postgresql.Driver")
    conn.setProperty("user", "postgres")
    conn.setProperty("password", "admin")
    conn.setProperty("url", url)
    
    val movie_genre_info = "movie_genre_info"
    val movieGenreDF = spark.read.jdbc(url, movie_genre_info, conn)
    //println("COUNT OF THE TABLE+++++++++++++++++++:::::"+movieGenreDF.count())
    val movie_rating= "movie_rating"
    val movieRatingDF = spark.read.jdbc(url, movie_rating, conn)
    
    val leastPopMovie = movieRatingDF.filter(col("rating")===1).dropDuplicates("user_id")
    println("COUNT AFTER DEDUP+++++++++++++++++::::::::::::::::" + leastPopMovie.count())
    
    val mostPopMovie = movieRatingDF.filter(col("rating")===5).dropDuplicates("user_id")
    
    println("@@@@@@@@@@@@@@@@@@COUNT AFTER DEDUP MOST POP MOVIE::::::::::::" + mostPopMovie.count())
    
    
    val df1 = leastPopMovie.union(mostPopMovie)
    
    val mostAndLeastPopMovie_info = "mostAndLeastPopMovie_info"
    
    df1.write.jdbc(url, mostAndLeastPopMovie_info, conn)
    
    
  }
  
  
}