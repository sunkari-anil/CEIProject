package CEIProject.org.ceiproject.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
//import scala.collection.parallel.ParIterableLike.SplitAt
import org.apache.commons.math3.geometry.spherical.oned.ArcsSet.Split
import java.util.Properties


object topMoviebyRatingAndGenre {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Top Movie By Rating And Genre").setMaster("local[*]")
                              .set("org.postgresql.Driver","C:/Users/sai kumar/Downloads/jar_files/postgresql-42.2.6.jar")
                              
                              
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
      
    val schema = StructType(Array(
                                 StructField("user_id",IntegerType),
                                 StructField("Item_id",IntegerType),
                                 StructField("rating",IntegerType),
                                 StructField("utime",StringType)))
       
    val uItemschema = StructType(Array(StructField("movie_id",IntegerType),
                                      StructField("movie_title",StringType),
                                      StructField("release_date",StringType),
                                      StructField("video_release_date",StringType),
                                      StructField("IMDb_URL",StringType),
                                      StructField("unknown",IntegerType),
                                      StructField("Action",IntegerType),
                                      StructField("Adventure",IntegerType),
                                      StructField("Animation",IntegerType),
                                      StructField("Childrens",IntegerType),
                                      StructField("Comedy",IntegerType),
                                      StructField("Crime",IntegerType),
                                      StructField("Documentary",IntegerType),
                                      StructField("Drama",IntegerType),
                                      StructField("Fantasy",IntegerType),
                                      StructField("Film_Noir",IntegerType),
                                      StructField("Horror",IntegerType),
                                      StructField("Musical",IntegerType),
                                      StructField("Mystery",IntegerType),
                                      StructField("Romance",IntegerType),
                                      StructField("Sci_Fi",IntegerType),
                                      StructField("Thriller",IntegerType),
                                      StructField("War",IntegerType),
                                      StructField("Western",IntegerType)))

     val uUserSchema = StructType(Array(StructField("user_id",IntegerType),
                                        StructField("age",IntegerType), 
                                        StructField("gender",StringType), 
                                        StructField("occupation",StringType),
                                        StructField("zip_code",IntegerType)))  
  
       
   //Postgres SQL Conncetion Details
                                        
    val conn = new Properties()
   // conn.setProperty("url", "jdbc:postgresql://localhost:5432/raw")    
    conn.setProperty("user", "postgres")
    conn.setProperty("password", "admin")
    conn.setProperty("Driver", "org.postgresql.Driver")
    
    val url = "jdbc:postgresql://localhost:5432/raw"
    val movie_rating= "movie_rating"
                                        
   // val schema1 = StructType(Array(StructField("user_data",StringType)))
   // val uFile = spark.sparkContext.textFile("file:///Users/sai kumar/Documents/Practice/CEI_Prac/MovieLens Data/MovieLens Data/u.data")
    
    val uFileDF = spark.read.format("csv").option("inferSchema","true").option("header","false").option("delimiter","\\t").schema(schema).load("file:///Users/sai kumar/Documents/Practice/CEI_Prac/MovieLens Data/MovieLens Data/u.data")
    
    
    val uItemDF = spark.read.format("csv").option("inferSchema","true").option("header","false").option("delimiter","|").schema(uItemschema).load("file:///Users/sai kumar/Documents/Practice/CEI_Prac/MovieLens Data/MovieLens Data/u.item")
   
   
    val uUserDF = spark.read.format("csv").option("inferSchema","true").option("header","false").option("delimiter","|").schema(uUserSchema).load("file:///Users/sai kumar/Documents/Practice/CEI_Prac/MovieLens Data/MovieLens Data/u.user")
   
    val parquetFile = spark.read.format("parquet").load("file:///Users/sai kumar/Downloads/userdata1.parquet")
    
    uFileDF.show(false)
    //parquetFile.show(false)
   // println("++++++++++Printing UDATA++++++++++++++++++++")
   // uFileDF.show(false)
    //println("*************Printing UINFO******************")
   
   // uItemDF.show(false)

    //println("##################Printing USER INFO######################")
    
   // uUserDF.show(false)

  //  uFileDF.groupBy(col("rating")).agg(col("rating")).select(col("rating")).show(false)
    
    println("LOADING USER RATING DATA INTO POSTGRESQL")
    
  //  uFileDF.write.jdbc(url, movie_rating, conn)
    uFileDF.write
    .format("jdbc")
    .option("url","jdbc:postgresql://localhost:5432/raw")
    .option("dbtable","movie_rating")
    .option("user", "postgres")
    .option("password","admin")
    .mode("overwrite")
    .save()
//    
//        uItemDF.write
//    .format("jdbc")
//    .option("url","jdbc:postgresql://localhost:5432/raw")
//    .option("dbtable","movie_genre_info")
//    .option("user", "postgres")
//    .option("password","admin")
//    .mode("overwrite")
//    .save()
//   
//           uUserDF.write
//    .format("jdbc")
//    .option("url","jdbc:postgresql://localhost:5432/raw")
//    .option("dbtable","user_info")
//    .option("user", "postgres")
//    .option("password","admin")
//    .mode("overwrite")
//    .save()
    
    val uFileDFPS = spark.read.jdbc(url, movie_rating, conn)
    //val stgData = spark.sql(s"""select * from movie_rating""")
  //  uFileDFPS.show(false)
    
   val uFileDFPSDF= uFileDFPS.withColumn("timestamp_val",from_unixtime(col("utime"),"yyyy-mm-dd HH:MM:SS"))
   
   println("COUNT OF THE MOVIE RATING TABLE DATA ::::::::::::::::::::::::::::::::::::::::::::::"+uFileDFPSDF.count())
   println(":::::::::::::::::::::::::::::::::::::OVERWRITTING THE DATA INTO MOVIE RATING TABLE::::::::::::::::::::::::::::::::::::::::::::::::::")
   
   val transDateTbl = "transdata_tbl"
   
   uFileDFPSDF.write.mode("overwrite").jdbc(url, transDateTbl, conn)
   
   val uFileDFPSDF1= uFileDFPSDF.filter(col("rating")===5)
   
   uFileDFPSDF1.createOrReplaceGlobalTempView("uFileDFPSDF1_vw")
   
   val df1 = spark.sql(s"select * from global_temp.uFileDFPSDF1_vw")
   
   val df2 = spark.sql(s"select Item_id,rating from global_temp.uFileDFPSDF1_vw group by Item_id,rating")
   
   
  // println("*****COUNT FROM VIEW DATA****:::::" + df2.count())
   
   
   //select Item_id,rating from movie_rating where rating=5 group by "Item_id",rating ;
   println("+++++++++++++++++++++++++Printing VIEW DATA+++++++++++++++++++++++++++++++")
   //df1.show(false)
   
  // println("TOP RATED MOVIE COUNT:::::"+ uFileDFPSDF1.count())
   
   //read data from movie_genre_info tableval
   val movie_genre_info_tbl_nm = "movie_genre_info"
   
   val movie_genre_info_tbl = spark.read.jdbc(url,movie_genre_info_tbl_nm,conn)
   
   println("@@@@@@@@@@@@@@MOVIE GENRE INFO TABLE@@@@@@@@@@@@@@@@@@@@")
  // movie_genre_info_tbl.show(false)
   
   //JOING movie_genre_info_tbl and df2 by using item_id and movie_id
   
   var joinDF = df2.join(movie_genre_info_tbl,df2("item_id")===movie_genre_info_tbl("movie_id"),"inner")
   
  joinDF= joinDF.drop("item_id","release_date","video_release_date","IMDB_URL")
 
  //joinDF.show(false)
  //joinDF=
  // .drop(col("release_date")).drop(col("video_release_date")).drop(col("IMDB_URL"))
   
  // drop(col("item_id"),col("release_date"),col("video_release_date"),col("IMDB_URL"))
  
  println("**********GET THE NUMBER OF PARTITIONS********************" + joinDF.toJavaRDD.getNumPartitions)
  
  //joinDF.repartition(1)
  
  val top_rated_movie_info = "top_rated_movie_info"
  //joinDF.repartition(1).write.option("header", "true").csv("file:///Users/sai kumar/Documents/Practice/movie_data_20230209")
  
  //joinDF.write.jdbc(url, top_rated_movie_info, conn)
  
 // joinDF.write.format("parquet").mode("append")
//  val userData = "user_data_parquet"
//  parquetFile.write.jdbc(url, userData, conn)
  
  
  }
  
}