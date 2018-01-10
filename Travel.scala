
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import scala.io.Source
import scala.collection.mutable.HashMap
import java.io.File
import org.apache.log4j.Logger
import org.apache.log4j.Level
import sys.process.stringSeqToProcess
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext._

object Travel {
  
  def main(args: Array[String]) 
  {
     //Create conf object
 val conf = new SparkConf()
 .setAppName("WordCount").setMaster("local[2]").set("spark.executor.memory","1g");
  
 //create spark context object
       val sc = new SparkContext(conf)
       val sql= new org.apache.spark.sql.SQLContext(sc)
       import sql.implicits._
       
       val travelData=sc.textFile("C:\\Users\\Parab\\Desktop\\acadglid\\new\\s18_rdd cont\\S18_Dataset.txt")
       val travels=travelData.map(_.split(",")).map(r=> TravelData(r(0).toInt,r(1),r(2),r(3),r(4).toInt,r(5).toInt))
       val travelsDF=travels.toDF();
       
       val userData=sc.textFile("C:\\Users\\Parab\\Desktop\\acadglid\\new\\s18_rdd cont\\S18_Dataset_User_details.txt")
       val users=userData.map(_.split(",")).map(r=> UserData(r(0).toInt,r(1),r(2).toInt))
           val userDF=users.toDF();
       userDF.registerTempTable("userDF")
      
      //1st
       
      travelsDF.filter(($"mode").like("airplane%")).groupBy("year").count().show()
      
      //2nd
      
      val userPerYearDistance= travelsDF.groupBy("user","year").sum("distance").alias("aa")//.sort("user", "year")
      userPerYearDistance.registerTempTable("userPerYearDistance")
      val finalResult=sql.sql("select  u.userName,t.* from userPerYearDistance t join userDF u on t.user=u.userId order by t.user,t.year").collect()
      finalResult.foreach(println)  
      
      
      //3rd
      
      val userDistance=travelsDF.groupBy("user").sum("distance")
      val maxDist=(userDistance.agg(Map("sum(distance)" -> "max")).collect())(0).getLong(0)
      val userMostDistance=userDistance.filter($"sum(distance)">=maxDist)
      userMostDistance.show()
      
      
       //4th
      val prefDest=travelsDF.groupBy("destination").count().sort(($"count").desc);
      prefDest.show(1);
  }
  case class UserData(userId:Int, userName:String, userAge:Int)
  
  case class TravelData(user: Int, source:String, destination:String, mode:String, distance:Int, year:Int);
}