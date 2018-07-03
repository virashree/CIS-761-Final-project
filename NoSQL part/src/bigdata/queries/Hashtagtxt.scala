package bigdata.queries

import com.google.gson.{GsonBuilder, JsonParser}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.util.parsing.json._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import scala.io.Source
case class Record(name: String)

object Hashtagtxt {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    
    //String to store all Hashtags Line from File
    var hashtags = ""
    
    val filename = "Users/Eng-YasserZalah/BigDataPhase/BigDataProject/HashtagsTopics.txt"
    
    //Read All lines from HashtagsTopics.txt and store each line in String "hashtags"
    for (line <- Source.fromFile(filename).getLines()) {
       hashtags =hashtags+(line.replace("#", ""))+"\n"
    }
     
    var path = "/Users/Eng-YasserZalah/BigDataPhase/BigDataProject/HashtagsTopics_New.txt"
    
    //Write String "hashtags" to new .txt file.
    scala.tools.nsc.io.File(path).writeAll(hashtags)
    
    //Create a new dataframe from the written file AS "HashtagsTopics"
    val peopleDF = spark.sparkContext
    .textFile(path)
    .map(_.split("\n"))
    .map(attributes => Record(attributes(0)))
    .toDF()
    peopleDF.createTempView("HashtagsTopics")
    
    //Load our data to create a TempView
     val df = spark.read.json("/Users/Eng-YasserZalah/BigDataPhase/BigDataProject/ManhattanWeather.json")
     df.createTempView("tb1")
     
     //Select the Hashtags on our data and create a new TempView for Hashtags only.
     val new1 = spark.sql("SELECT hashtags.text, COUNT(*) as counts FROM tb1 LATERAL VIEW EXPLODE(entities.hashtags) AS hashtags").toDF()
     new1.show()
     //new1.createTempView("TopHashtags")
     
     //Inner Join the two tables 
     //val join2 = sqlContext.sql("SELECT distinct(TopHashtags.text) FROM TopHashtags INNER JOIN HashtagsTopics ON TopHashtags.text=HashtagsTopics.name")
     //println(join2.count())
     //join2.show()
  }
}