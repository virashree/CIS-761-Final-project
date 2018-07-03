package bigdata.queries

import com.google.gson.{GsonBuilder, JsonParser}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object query4 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df = spark.read.json("/Users/Eng-YasserZalah/BigDataPhase/BigDataProject/ManhattanWeather.json")
    df.createOrReplaceTempView("tb4")

    //will list count of #weather and #climate
    val sql4 = spark.sql("SELECT count(*) total_Tweets, sum(case when lower(text) like '%weather%' then 1 else 0 end)weather, sum(case when lower(text) like '%climate' then 1 else 0 end) climate from tb4")
    sql4.show()
  }
}