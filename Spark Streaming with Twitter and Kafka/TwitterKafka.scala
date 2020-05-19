import java.util.Properties
import Sentiment.Sentiment
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import scala.collection.convert.wrapAll._

object TwitterKafka {
  val properties = new Properties()
  properties.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(properties)

  def main(args: Array[String]) {

    if (args.length != 2) {
      println("Enter 2 arguments")
      println("Usage: First_argument - Tweets Topic, Second Argument - Kafka Topic")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("Spark_Streaming_with_Twitter_Kafka")
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[*]")
    }

    val topic = args(1)
    System.setProperty("twitter4j.oauth.consumerKey", "TlL3kVGE9ibWmkolVXE1wHX9x")
    System.setProperty("twitter4j.oauth.consumerSecret", "8P5YGIsI4bfgDjKGH83jOyKCY4EJ1UkU07xLVLzU4YTM4IH8ct")
    System.setProperty("twitter4j.oauth.accessToken", "1245860552401924097-U6FCmi961DvCA9w9XQMVoaFxX9B5xC")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "XhdmZnpPZOvTD2G9CuOE1oLvQYHEgZyzKiqwROhgkfyV5")

    val searchQuery = Seq(args(0))
    val streamingContext = new StreamingContext(sparkConf, Seconds(60))
    val getTweets = TwitterUtils.createStream(streamingContext, None, searchQuery)
    val tweetSentiment = getTweets.map(status =>status.getText()).map{hashTag=> (hashTag,getSentiment(hashTag))}

    tweetSentiment.cache().foreachRDD(sentimentRDD =>
      sentimentRDD.foreachPartition(partition =>
        partition.foreach { x => {
          val sparkSerializer = "org.apache.kafka.common.serialization.StringSerializer"
          val properties = new Properties()
          properties.put("bootstrap.servers", "localhost:9092")
          properties.put("key.serializer", sparkSerializer)
          properties.put("value.serializer", sparkSerializer)
          val producer = new KafkaProducer[String, String](properties)
          val consumerData = new ProducerRecord[String, String](topic, x._1.toString,x._2.toString)
          println(x)
          producer.send(consumerData)
          producer.close()
        }}
      ))
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  private def sentimentsRange(text: String): List[(String, Sentiment)] = {
    val annotation: Annotation = pipeline.process(text)
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    sentences.map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree])))
      .map { case (sentence, tree) => (sentence.toString, Sentiment.toSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
      .toList
  }

  private def getSentiment(text: String): Sentiment = {
    val list = sentimentsRange(text)
    val (_, sentiment) = if(list.nonEmpty) list
      .maxBy { case (sentence, _) => sentence.length }
    else sentimentsRange("key").maxBy{case (sentence, _) => sentence.length }
    sentiment
  }

  def matchSentiment(input: String): Sentiment = Option(input) match {
    case Some(text) if text.nonEmpty => getSentiment(text)
    case _ => throw new IllegalArgumentException("Invalid input")
  }

  def listSentiment(input: String): List[(String, Sentiment)] = Option(input) match {
    case Some(text) if text.nonEmpty => sentimentsRange(text)
    case _ => throw new IllegalArgumentException("Invalid input")
  }

}