package bigdata.queries
import com.google.gson.{GsonBuilder, JsonParser}
import twitter4j.TwitterFactory
import twitter4j.Twitter
import twitter4j.User
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.util.control._

object twtAPI {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
     val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    import sqlContext.implicits._
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
      
      val loop = new Breaks;
      var x = 0

    loop.breakable {
        while(true){
      println("Hellow Type 1 to go")
      x  = scala.io.StdIn.readLine().toInt
    
      if(x == 1){
        println("Starting TWT Function")
        val test = twtquery()
        loop.break;
      }else{
        println("Type 1 to start the Function...")
      }
     }
    }
  
    def twtquery() {
      
    val df = spark.read.json("/Users/Eng-YasserZalah/BigDataPhase/BigDataProject/ManhattanWeather.json")
    df.createOrReplaceTempView("tb5")
    val userql = sqlContext.sql("SELECT user.name, user.id from tb5 where user.followers_count > 1000000 AND user.verified = true")
    userql.show()
    var userid = ""
    var username = ""
    userid = userql.first()(1).toString()
    username = userql.first()(0).toString()
    println("Selcted User Name for This Name -"+username+"-")
    println("Selcted User ID is for This Name -"+userid+"-\n")

    var twtuserID = userid.toLong

    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey("h5aqUcgc8Z69h3c0dVoliR9md")
      .setOAuthConsumerSecret("Zrpu4lBPKL6lMu8VX1xFSSFYKvjWdYLKPOuXwlgZkyUJ3mlUwb")
      .setOAuthAccessToken("403426448-bndEijf9IWcth53qU0gk4rqkK5pv9wu0vsDh41kG")
      .setOAuthAccessTokenSecret("rxx5A0LNFArYlQQed2H2flzm6xJujslxoBT6Ag1Q3ASnC")
    val tf = new TwitterFactory(cb.build())
    val twitter = tf.getInstance()

    val statuses = twitter.getUserTimeline(twtuserID)
    val it = statuses.iterator()
  
    val test2 =it.next()
    println(username+" Description: "+test2.getUser.getDescription+"\n");
    println("Loading Latest-20 "+username+" Tweets: \n")
    
    while (it.hasNext()) {
      val status = it.next()
      println(status.getUser().getName() + ": " + status.getText());
    }
		}
   }
}