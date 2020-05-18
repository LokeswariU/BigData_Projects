#AWS AccessKeyID and SecretAccessKey Credentials from the security credentials of AWS Account
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "")
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "")

# Logistic Regression Model for Predicting if a Patient has Pneumonia or Not, using Chest X-ray images downloaded from the Kaggle dataset
# The Chest X-Ray Images for Train Normal and Train Pneumonia Data and Images for Test Normal and Test Pneumonia are downloaded for training and testing the Logistic Regression Model 
# Upload the Chest X-Ray Train and Test Dataset in the S3 Bucket of AWS.

# Create libraries for spark-deep-learning:1.5.0-spark2.4-s_2.11, tapanalyticstoolkit:spark-tensorflow-connector:1.0.0-s_2.11,databricks:tensorframes:0.8.2-s_2.11 from Maven
# sparkdl, keras, tensorflow,Pillow libraries are installed from Pypi
# Loading all the libraries needed for building a pipeline for Logistic Regression Model

from pyspark.sql import functions as F
from pyspark.sql.functions import lit
import tensorflow as tf
import keras
from PIL import Image
import sparkdl
from pyspark.ml.image import ImageSchema
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from sparkdl import DeepImageFeaturizer

# Loading Train Images and creating Labels as 0 for Normal and 1 for Pneumonia images

Train_normal = ImageSchema.readImages("s3://image-class/train/NORMAL/")
Train_pneumonia = ImageSchema.readImages("s3://image-class/train/PNEUMONIA/")
Train_normal = Train_normal.withColumn("label", lit(0))
Train_pneumonia = Train_pneumonia.withColumn("label", lit(1))

# Combining all the Train Images into a single Train Dataset
Train_images = Train_pneumonia.union(Train_normal)

# Example of a Pneumonia Image loaded from the S3 Bucket
Train_pneumonia.first()

# Pictures of the Train Normal Chest X-Ray loaded from the S3 Bucket
display(Train_normal)

# Loading Test Images and Creating Labels as 0 for Normal Images and 1 for Pneumonia Images Test data
Test_normal = ImageSchema.readImages("s3://image-class/test/NORMAL/")
Test_pneumonia = ImageSchema.readImages("s3://image-class/test/PNEUMONIA/")

Test_normal = Test_normal.withColumn("label", lit(0))
Test_pneumonia = Test_pneumonia.withColumn("label", lit(1))

# Combining all the Test Normal and Pneumonia Images into a Test Dataset
Test_images = Test_pneumonia.union(Test_normal)

# Using Spark-deeplearning library for feature selection and mapping
# Using Logistic regression function of Pyspark.ml library for building the model with a Pipeline

featurizer = DeepImageFeaturizer(inputCol="image", outputCol="features", modelName="InceptionV3")
lr = LogisticRegression(maxIter=15, regParam=0.7, elasticNetParam=0.3, labelCol="label")
p = Pipeline(stages=[featurizer, lr])

#Train the model by limiting 1500 Train Images
model = p.fit(Train_images.limit(1500))   

# Testing the model with the Test dataset using Transform function 
# This produces an output of Images, Predicted_label and Actual Label

df = model.transform(Test_images.limit(1000)).select("image", "prediction",  "label")
predictionAndLabels = df.select("prediction", "label")


# Using MulticlassEvaluator to get the Accuracy, Precision, Recall and F-Statistics
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
f1 = MulticlassClassificationEvaluator(metricName = "f1")
recall = MulticlassClassificationEvaluator(metricName = "weightedRecall")
precision = MulticlassClassificationEvaluator(metricName = "weightedPrecision")

print("Accuracy = " + str(evaluator.evaluate(predictionAndLabels)))
print("Precision = " + str(precision.evaluate(predictionAndLabels)))
print("F1-Statistics = " + str(f1.evaluate(predictionAndLabels)))
print("Recall = " + str(recall.evaluate(predictionAndLabels)))

#Shows the Images, Predicted Labels and Actual Labels

df.show()