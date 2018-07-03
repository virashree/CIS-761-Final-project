package bigdata.queries

import com.google.gson.{GsonBuilder, JsonParser}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.util.parsing.json._

object query9 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df = spark.read.json("/Users/Eng-YasserZalah/BigDataPhase/BigDataProject/ManhattanWeather.json")

    df.createOrReplaceTempView("tb1")

    
    //most common hashtags
    
    val sql5 = spark.sql("SELECT t.retweeted_screen_name, sum(retweets) AS total_retweets,count(*) AS tweet_count FROM (SELECT retweeted_status.user.screen_name as retweeted_screen_name,retweeted_status.text, max(retweet_count) as retweets FROM tb1 GROUP BY retweeted_status.user.screen_name, retweeted_status.text) t GROUP BY t.retweeted_screen_name  ORDER BY total_retweets DESC LIMIT 3")

    sql5.show()

    sql5.write.json("/Users/Eng-YasserZalah/BigDataPhase/BigDataProject/new.json")

  }

}