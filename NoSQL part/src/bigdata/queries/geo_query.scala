package bigdata.queries

import com.google.gson.{GsonBuilder, JsonParser}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object geo_query {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df = spark.read.json("/Users/Eng-YasserZalah/BigDataPhase/BigDataProject/ManhattanWeather.json")
    df.createOrReplaceTempView("tb5")

    //find the most favorite tweet.
    val sql5 = spark.sql("SELECT user.name,user.id,place.full_name,geo.coordinates[0],geo.coordinates[1] from tb5 where geo is not null")
    sql5.show()
    //sql5.write.json("/Users/Eng-YasserZalah/BigDataPhase/BigDataProject/new.json")
        
  }
}