package bigdata.queries

import com.google.gson.{GsonBuilder, JsonParser}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.util.parsing.json._

object query7 {

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


    //count the most active Time_zone
    val sql4 = spark.sql("SELECT user.time_zone, COUNT(*) as cnt from tb1 where lang is not null and user.time_zone is not null GROUP BY user.time_zone ORDER BY cnt DESC")
    sql4.show()
    


  }

}