package bigdata.queries

import com.google.gson.{GsonBuilder, JsonParser}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.util.parsing.json._

object query10 {

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
      val sql8 = spark.sql("SELECT LOWER(hashtags.text) As Hashtags, COUNT(*) AS total_count FROM tb1 LATERAL VIEW EXPLODE(entities.hashtags) t1 AS hashtags GROUP BY LOWER(hashtags.text) ORDER BY total_count DESC LIMIT 20")
    sql8.show()


  }

}