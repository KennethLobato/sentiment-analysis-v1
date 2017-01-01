package es.deusto.bigdata

import java.util
import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.trees.Tree
import edu.stanford.nlp.util.CoreMap
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import twitter4j.TwitterObjectFactory

import scala.collection.JavaConverters._


/**
  * Created by klobato on 29/12/2016.
  */
object SentimentAnalysis {

  def sentimentAnalysis(text: String) : String = {
    //Initialize CoreNLP
    val corenlpProps = new Properties()
    corenlpProps.setProperty("annotators", "tokenize,ssplit,pos,lemma,parse,sentiment")
    val pipeline: StanfordCoreNLP = new StanfordCoreNLP(corenlpProps)

    val annotation: Annotation = pipeline.process(text)

    var longest = 0;
    var mainSentiment = 0;

    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation]).asScala

    for (sentence <- sentences){
      val tree = sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree])
      val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
      val partText = sentence.toString()
      if(partText.length > longest){
        mainSentiment = sentiment
        longest = partText.length()
      }
    }

    if(mainSentiment == 0 || mainSentiment == 1)
      return "NEGATIVE"
    else if (mainSentiment == 2)
      return "NEUTRAL"
    else if (mainSentiment == 3 || mainSentiment == 4)
      return "POSITIVE"
    else
      return "UNKNOWN"
  }

  def main(args: Array[String]): Unit = {

    if(args.length < 5){
      System.err.println("Usage: [local[*]/yarn-cluster] [zkQuorum] [group] [topics] [numThreads] [resultsHdfsPath]")
      System.exit(1)
    }

    //Parse args
    val Array(master, zkQuorum, group, topics, numThreads, hdfsPath) = args

    //Start SparkContext and StreamingContext
    val conf = new SparkConf().setAppName("SentimentAnalysis")
    conf.setMaster(master)
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    //Create a Checkpoing Path
    ssc.checkpoint(".checkpoint")

    //Start receiving messages from Kafka
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)

    val tweetsStream = kafkaStream.map(_._2)
      .map(strJson => TwitterObjectFactory.createStatus(strJson)) //Convert JSON string to Twitter4j Status entity
      .map{status =>
       (status.getId(),sentimentAnalysis(status.getText().toString()), status.getText())
      }
    tweetsStream.print()
    //Save information without tweet text, because RAW tweet is already stored in HDFS by Flume
    tweetsStream.map{v => (v._1,v._2)}.saveAsTextFiles(hdfsPath)

    ssc.start()
    ssc.awaitTermination()
  }
}
