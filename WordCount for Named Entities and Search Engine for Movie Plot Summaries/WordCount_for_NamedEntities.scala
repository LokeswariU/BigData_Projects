// Assignment Part 1 - WordCount for Named Entities

//Importing all the Johnsnow libraries for pipeline
import com.johnsnowlabs.nlp._
import com.johnsnowlabs.nlp.annotators._

//Importing all the Johnsnow libraries for pipeline
import com.johnsnowlabs.nlp._
import com.johnsnowlabs.nlp.annotators._
import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.annotators.ner.NerConverter
import com.johnsnowlabs.nlp.base._
import com.johnsnowlabs.util.Benchmark
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline

// Importing for using tokenizer
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.sql.functions._

// Getting values from TwoTaleCities text book
val text = spark.read.option("header", false).csv("/FileStore/tables/Tale_TwoCities.txt").select($"_c0".as("text"))

// Pipelining the text values and converting into Named entities
val pipeline = PretrainedPipeline("onto_recognize_entities_sm", lang = "en")
val Named_Entity = pipeline.transform(text).select("entities.result")


// Selecting the text column as result column from the Named_entities RDD
val Named_Entity_RDD = Named_Entity.select(explode(col("result")).as("text")).rdd

// Displaying all the named_entites of the document
Named_Entity_RDD.collect()

// Total Count of the Named_entities in the whole document
val count = Named_Entity_RDD.map(x => (x,1))
count.collect()

// Sorting the Named_entities based on the count of each Entity
val Reduce_entity = count.reduceByKey((x,y) => x+y).sortBy(-_._2)
Reduce_entity.collect()

