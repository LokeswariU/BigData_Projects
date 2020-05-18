import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import collection.mutable.Map
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.evaluation.MulticlassMetrics

object TwitterSentimentAnalysis {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: Tweet_dataset-InputDir, Metrics-OutputDir")
    }

    val spark = SparkSession.builder.appName("Twitter Processing and Classification").master("local").getOrCreate()
    val sc = spark.sparkContext
    val sql = spark.sqlContext
    import spark.implicits._
    val data = spark.read.option("header","true").option("inferschema","true").csv(args(0)).filter($"text".isNotNull)
    val Array(train,test) = data.randomSplit(Array(0.6,0.4))
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("tokens")
    val stopWordsRemover = new StopWordsRemover().setInputCol(tokenizer.getOutputCol).setOutputCol("withoutStopWords")
    val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(stopWordsRemover.getOutputCol).setOutputCol("rawfeatures")
    val indexer = new StringIndexer().setInputCol("airline_sentiment").setOutputCol("label")
    val lr = new LogisticRegression().setMaxIter(15).setLabelCol("label").setFeaturesCol("rawfeatures")
    val pipeline = new Pipeline().setStages(Array(tokenizer,stopWordsRemover, hashingTF,indexer,lr))
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction")
    val crossValidator = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    val model = crossValidator.fit(train)
    val result = model.transform(test)
    val prediction = result.select("prediction", "label").rdd.map{case Row(prediction: Double, label: Double) => (prediction, label)}
    val metrics = new MulticlassMetrics(prediction)
    val labels = metrics.labels
    var metrics_output = "\n Metrics for Logistic Regression \n"

    metrics_output+= "\n Accuracy = " + metrics.accuracy.toString
    metrics_output+= "\n Weighted Precision = "+metrics.weightedPrecision.toString
    metrics_output+= "\n Weighted Recall = "+metrics.weightedRecall.toString
    metrics_output+= "\n Weighted Fmeasure = "+metrics.weightedFMeasure.toString
    metrics_output+= "\n Confusion Matrix : \n"+metrics.confusionMatrix.toString
    metrics_output+= "\n Weighted TruePositive = "+metrics.weightedTruePositiveRate.toString
    metrics_output+= "\n Weighted FalsePositive = "+metrics.weightedFalsePositiveRate.toString
    metrics_output+="\n Label wise Data"
    for ( i<- labels) {
      metrics_output+="\n\t Label :"+i.toString
      metrics_output+= "\n TruePositive = "+metrics.truePositiveRate(i).toString
      metrics_output+= "\n FalsePositive = "+metrics.falsePositiveRate(i).toString
      metrics_output+= "\n Precision = "+metrics.precision(i).toString
      metrics_output+= "\n Recall = "+metrics.recall(i).toString
    }

    sc.parallelize(List(metrics_output)).coalesce(1,true).saveAsTextFile(args(1))

  }
}