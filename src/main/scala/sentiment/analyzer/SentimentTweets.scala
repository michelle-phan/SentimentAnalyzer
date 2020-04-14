package sentiment.analyzer

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.functions.udf

object SentimentTweets {

  def main(args: Array[String]) {
    if (args.length < 3) {
      println("Usage: Please enter yesterday time: yyyy mm dd")
      System.exit(1)
    }
    
    val appName = "SentimentTweets"
    val conf = new SparkConf()
    conf.setAppName(appName).setMaster("local[3]")
    val spark = SparkSession.builder().appName(appName).config(conf).getOrCreate()

    val year = args(0);
    val month = args(1);
    val day = args(2);
    
    import spark.implicits._
    val jsonDF = spark.read.format("json").load(s"hdfs://localhost:9000/datalake/raw/twitter/year=$year/month=$month/day=$day")
    val jsonDF1 = jsonDF.filter($"lang" === "en")
//    val jsonDF1_2 = jsonDF1_1.select($"text" as "text1").rdd.map {
//      case Row(text: String) => (text, test(text).toString)
//    }.toDF("text1", "sentiment")
    
    val udfSentiment = udf(SentimentAnalyzer.mainSentiment)
    val jsonDF2 = jsonDF1.withColumn("sentiment", udfSentiment($"text"))
    
    jsonDF2.write.format("parquet").save(s"hdfs://localhost:9000/datalake/preprocessed/twitter/sentiment/year=$year/month=$month/day=$day/tweetsWithSentiment.parquet")
  }
}