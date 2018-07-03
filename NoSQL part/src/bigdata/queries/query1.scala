
package bigdata.queries

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.util.parsing.json._

object query1 {

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
    val sqlDF = spark.sql("SELECT user.name from tb1 where user.followers_count > 30000 AND user.verified = true ")
    sqlDF.show()

  }

}