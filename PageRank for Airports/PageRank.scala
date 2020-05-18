import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.array
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import collection.mutable.Map
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object PageRank {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage: Airports-InputDir, Iteration_count,Airports_rank-OutputDir")
    }
    val spark = SparkSession.builder.appName("Page Rank Implementation").master("local").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val start = 10.0;
    val airports = spark.read.format("csv").option("header","true").load(args(0))
    val destination = args(2).toString
    val outlinks = airports.groupBy("ORIGIN").count()
    val inlinks = airports.groupBy("DEST").count()
    val out = airports.select("ORIGIN").distinct()
    val in = airports.select("DEST").distinct()
    val iteration_count =args(1).toInt
    val Unique = airports.select(explode(array("ORIGIN", "DEST")).alias("Unique")).dropDuplicates()
    val UniqueAirports = Unique.select("Unique").collect.map(_.getString(0))
    val keyValue = outlinks.rdd.map(row => (row.getString(0),row.getLong(1)))
    val mapping = keyValue.collect().toMap
    val origin = airports.select("ORIGIN","DEST").rdd.map(x => (x(0).toString, x(1).toString)).collect

    val rank = Map() ++ UniqueAirports.map(x => (x, start)).toMap;
    for (i <- 1 to iteration_count) {
      val calculate = Map() ++ UniqueAirports.map(x => (x, 0.0)).toMap
      rank.keys.foreach((id) => rank(id) = rank(id) / mapping(id))
      for ((key, value) <- origin) {
        calculate.put(value, calculate(value) + rank(key))
      }
      val pr_out = collection.mutable.Map() ++ calculate.map(x => (x._1, ((0.15 / UniqueAirports.size) + 0.85 * x._2)))
      pr_out.keys.foreach((id) => rank(id) = pr_out(id))
    }
    val results = rank.toSeq.sortBy(-_._2).toDF("Airport", "Rank")
    results.write.csv(destination)

  }
}