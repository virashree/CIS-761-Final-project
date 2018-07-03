
package bigdata.queries

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.util.parsing.json._

object query6 {

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
      
      println(df.count())
      // Count devices or Softwares which have used to tweet by users.
      val sql4 = spark.sql("SELECT sum(case when lower(source) like '%android%' then 1 else 0 end)android, sum(case when lower(source) like '%iphone%' then 1 else 0 end)iphone, sum(case when lower(source) like '%ipad%' then 1 else 0 end)ipad, sum(case when lower(source) like '%web%' then 1 else 0 end)web, sum(case when lower(source) like '%tweetdeck%' then 1 else 0 end)Tweetdeck, sum(case when lower(source) like '%tweetbot%' then 1 else 0 end)Tweetbot, sum(case when lower(source) like '%www.sprinklr.com%' then 1 else 0 end)Sprinklr, sum(case when lower(source) like '%www.socialnewsdesk.com%' then 1 else 0 end)SocialNewsDesk, sum(case when lower(source) like '%instagram.com%' then 1 else 0 end)Instagram, sum(case when lower(source) like '%facebook.com%' then 1 else 0 end)Facebook from tb1")
      sql4.show()
     
      
  // val sql5 = spark.sql("SELECT sum(case when lower(retweeted_status.source) like '%android%' then 1 else 0 end)android, sum(case when lower(retweeted_status.source) like '%iphone%' then 1 else 0 end)iphone, sum(case when lower(retweeted_status.source) like '%ipad%' then 1 else 0 end)ipad, sum(case when lower(retweeted_status.source) like '%web%' then 1 else 0 end)web, sum(case when lower(retweeted_status.source) like '%tweetdeck%' then 1 else 0 end)Tweetdeck, sum(case when lower(retweeted_status.source) like '%tweetbot%' then 1 else 0 end)Tweetbot, sum(case when lower(retweeted_status.source) like '%www.sprinklr.com%' then 1 else 0 end)Sprinklr,  sum(case when lower(retweeted_status.source) like '%www.socialnewsdesk.com%' then 1 else 0 end)SocialNewsDesk, sum(case when lower(retweeted_status.source) like '%instagram.com%' then 1 else 0 end)Instagram,sum(case when lower(retweeted_status.source) like '%facebook.com%' then 1 else 0 end)Facebook  from tb1")
    //  sql5.show()
      
      
 //   val sql6 = spark.sql("SELECT sum(case when lower(quoted_status.source) like '%android%' then 1 else 0 end)android, sum(case when lower(quoted_status.source) like '%iphone%' then 1 else 0 end)iphone, sum(case when lower(quoted_status.source) like '%ipad%' then 1 else 0 end)ipad, sum(case when lower(quoted_status.source) like '%web%' then 1 else 0 end)web, sum(case when lower(quoted_status.source) like '%tweetdeck%' then 1 else 0 end)Tweetdeck, sum(case when lower(quoted_status.source) like '%tweetbot%' then 1 else 0 end)Tweetbot, sum(case when lower(quoted_status.source) like '%www.sprinklr.com%' then 1 else 0 end)Sprinklr,  sum(case when lower(quoted_status.source) like '%www.socialnewsdesk.com%' then 1 else 0 end)SocialNewsDesk, sum(case when lower(quoted_status.source) like '%instagram.com%' then 1 else 0 end)Instagram, sum(case when lower(quoted_status.source) like '%facebook.com%' then 1 else 0 end)Facebook from tb1")
  // sql6.show()
      
  }
}