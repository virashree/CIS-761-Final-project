package bigdata.queries

import com.google.gson.{GsonBuilder, JsonParser}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.util.parsing.json._

object query2 {

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

    //count the most active languages
    val sql2 = spark.sql("SELECT lang, COUNT(*) as cnt from tb1  where lang is not null GROUP BY lang ORDER BY cnt DESC ")
    sql2.show()
    

  }

}