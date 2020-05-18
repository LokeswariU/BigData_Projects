// Reading MetaData of the Movies Summary Database
val metadata = sc.textFile("/FileStore/tables/movie_metadata-ab497.tsv")
val movies = metadata.map(x => (x.split("\t")(0), x.split("\t")(2)))

// Loading the stopwords text file with all the possible stopwords to be removed from the documents
val stopWordsInput = sc.textFile("/FileStore/tables/stopw.txt")
val stopWordsSet = stopWordsInput.flatMap(x => x.split(" ")).collect().toSet

// Summaries have all the movie summary data from the plot_summaries text file
val summaries = sc.textFile("/FileStore/tables/plot_summaries.txt").map(x => x.toLowerCase.split("""\s+"""))
val clean_summaries = summaries.map(x => x.map(y => y).filter(word => stopWordsSet.contains(word) == false)).map(x=>(x(0),x.drop(1)))
val movies = metadata.map(_.split("\t")).map(x => (x(0), x(2)))

// Adding the search terms from a file 
import scala.util.control.Breaks._
val search_queries = sc.textFile("/FileStore/tables/searchterms.txt").collect

// Summaries data with the Movie ID and the Description of the movie
clean_summaries.collect()

// Count of the summaries data i.e. the Number of movies
val count = clean_summaries.count

// Calculating the TF and IDF values for a search term in the list of documents
val TF = clean_summaries.flatMap(x => x._2.map(y => ((x._1, y), 1))).reduceByKey((x,y) => x+y).map(x => (x._1._2, (x._1._1, x._2)))    
val DF = TF.map(x => (x._1, 1)).reduceByKey((x,y) => x+y).map(x => (x._1, (x._2, math.log(count/x._2))))
var tf_idf = DF.join(TF).map(x => (x._2._2._1, (x._1, x._2._2._2, x._2._1._1, x._2._1._2, x._2._2._2 * x._2._1._2)))

tf_idf = movies.join(tf_idf).map(x => x._2)

// Calculating the Cosine Similarity for multiple search terms

def Cos_Similarity (Rdd_TF_IDF: RDD[(String, (String, Int, Int, Double, Double))], tokens: Array[String]) : Array[String] = {
  var Cos_TF = sc.parallelize(tokens).map(x => (x, 1)).reduceByKey((x,y) => x+y)
  var cosTfIdf = Cos_TF.leftOuterJoin(Rdd_TF_IDF.map(x => x._2).map(x => (x._1, (x._3, x._4)))).map(x => (x._1, if (x._2._2.isEmpty) 0 else x._2._1 * x._2._2.get._2))
  var Merge = Rdd_TF_IDF.map(x => (x._2._1, (x._1, x._2._5))).join(Cos_TF).map(x => x._2).map(x => (x._1._1, x._1._2, x._2))
  var Product_Map = Merge.map(x => (x._1, (x._2 * x._3, x._3 * x._3, x._2 * x._2))).reduceByKey((x,y) => ((x._1 + y._1, x._2 + y._2, x._3 + y._3)))
  return Product_Map.map(x => (x._1, x._2._1/(math.sqrt(x._2._2) * math.sqrt(x._2._3)))).sortBy(-_._2).map(_._1).take(10)
}

// Calculating the TF_IDF and taking as RDD file
def TF_IDF_RDD (Rdd_TF_IDF: RDD[(String, (String, Int, Int, Double, Double))], tokens: Array[String]) : Array[String] = {
  return Rdd_TF_IDF.filter(x => x._2._1 == tokens.head).sortBy(-_._2._5).map(_._1).take(10) 
}


// Outputs the Top 10 relevent movies based on each search term
import scala.util.control.Breaks._
val search_queries = sc.textFile("/FileStore/tables/searchterms.txt").collect
for (query_term <- search_queries) {
  println(query_term)
  var search_terms = query_term.split(" ").map(_.toLowerCase.trim)
  breakable {
    if (search_terms.length == 0) {
      break
    } else {
      var result = if (search_terms.length > 1) Cos_Similarity (tf_idf, search_terms) else TF_IDF_RDD(tf_idf, search_terms)
      println("Top 10 related movies based on the user's search terms ")
      result.foreach {println}
    }
  }
  println
}



