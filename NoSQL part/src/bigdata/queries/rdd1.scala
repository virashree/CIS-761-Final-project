package bigdata.queries

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

object rdd1 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    //val tweet = sc.textFile("C:/test2.json")
    val df = spark.read.json("/Users/Eng-YasserZalah/BigDataPhase/BigDataProject/ManhattanWeather.json")
    df.createOrReplaceTempView("tb1")
    //val rddFromSql = spark.sql("SELECT user.name,user.text FROM tb1 LIMIT 20").rdd

    val rddFromSql = spark.sql("SELECT user.name, text FROM tb1 LIMIT 20").rdd

    //val rows = tweet.map(line => line.split(","))
    val data = rddFromSql.map{case item:Row =>
        val name = item.getString(0)
        val text = item.getString(1)

      (name, text)
    }
    // To demonstrate the example, lets work with Arrays
    val dataArray = data.collect()

    dataArray.foreach{case (name, text) => println("Name =>-" + name + "-Twt Text => " + text )}
  }

}
