import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object graph_x {
	def main(args: Array[String]): Unit = {

		//Number of Arguments to be Given as Input
		if (args.length < 2) {
			println("Exception : Enter 2 Arguments : Input_file_path , Output_file_path")
		}

		val input_file = args(0)
		val output_file = args(1)
		val sc = new SparkContext(new SparkConf().setAppName("Social Network Analysis Using GraphX"))
		
		// Read the input file using textFile() and save as val google
		val google = sc.textFile(input_file)

		// Processing the Source and Destination data into edgesRDD
		// Converting the edgesRDD into Graph using only the edges with default vertex
		val edgesRDD:RDD[(VertexId,VertexId)] = google.map(line =>(line(0).toInt, line(1).toInt))
		val graph = Graph.fromEdgeTuples(edgesRDD,1)

		//Calculate the inDegrees and the outDegrees of the edges from the Graph
		val inDegrees: VertexRDD[Int] = graph.inDegrees
		val outDegrees: VertexRDD[Int] = graph.outDegrees

		val InDegree = inDegrees.sortBy(-_._2).take(5)
		val OutDegree = outDegrees.sortBy(-_._2).take(5)

		// Calculate the PageRank, ConnectedComponents, TriangleCount of the edges from the Graph
		val PageRank = graph.pageRank(0.01).vertices.sortBy(-_._2).take(5)
		val connectedComp = graph.connectedComponents().vertices.sortBy(-_._2).take(5)
		val TriangleCount = graph.triangleCount().vertices.sortBy(-_._2).take(5)

		// Saving all the five Analyzed Network data in the output textfile
		sc.parallelize(InDegree).coalesce(1,true).saveAsTextFile(output_file+"InDegrees.txt")
		sc.parallelize(OutDegree).coalesce(1,true).saveAsTextFile(output_file+"OutDegrees.txt")
		sc.parallelize(PageRank).coalesce(1,true).saveAsTextFile(output_file+"PageRank.txt")
		sc.parallelize(connectedComp).coalesce(1,true).saveAsTextFile(output_file+"ConnectedComponents.txt")
		sc.parallelize(TriangleCount).coalesce(1,true).saveAsTextFile(output_file+"TriangleCount.txt")
		
		}
	}
