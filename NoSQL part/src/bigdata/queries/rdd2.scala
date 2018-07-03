package bigdata.queries

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

object rdd2 {

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
    // need to make
    val rdd2=spark.sql("SELECT distinct(user.name), user.followers_count from tb1 where user.verified=true AND user.followers_count>10000 ORDER BY user.followers_count DESC").rdd

    val data =rdd2.map{case item:Row =>
      val name= item.getString(0)
      val followers = item.getLong(1)
      (name,followers)
    }
    val dataArray = data.collect()
    dataArray.foreach{case (name,followers) => println("Name =>-" + name + " --> Followers => " + followers)}
  }

}