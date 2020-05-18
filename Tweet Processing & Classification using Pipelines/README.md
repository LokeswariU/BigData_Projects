**Tweet Processing & Classification using Pipelines**

*The TweetProcessing model is written in TwitterSentimentAnalysis.scala file.*

*The appropriate Jar file is generated for the TwitterSentimentAnalysis.scala file using IntelliJ and the Input file – Tweets.csv.*

*The Jar file along with the input file ( Tweets.csv) should uploaded in the AWS S3 bucket.*

*Follow the steps to implement the TwitterSentimentAnalysis model in the AWS cluster.*

*The output from the AWS cluster is available as Tweets_output.txt.*


**Instruction for Generating Jar file using IntelliJ:**

1) Open a new Project in IntelliJ->select Scala->sbt->next Give the name for the project. 
2) If Scala Project is not available download the plugin for scala in IntelliJ IDEA.
3) Create a new Scala class in the Project folder -> src folder ->main folder ->scala folder in IntelliJ.
4) Download the TwitterSentimentAnalysis.scala file. Copy the file content in the newly created scala class.
5) In the Project folder -> target -> build.sbt is previously generated. Copy paste the contents of the build.sbt file or paste the .scala file directly in the (Project)main folder in the local computer location.
6) Run the scala class created in the IntelliJ.
7) Then Goto View tab -> Tool Windows -> click sbt.
8) In the Sbt panel opened in the right of the window, Goto PageRank -> sbt tasks -> click on Package.
9) After the sbt shell is run, the Jar file is generated in the Target folder -> Scala folder of the Project.

**Instructions for Running the Jar file in AWS:**

1) Create a S3 bucket in AWS. Upload the input file - Tweets.csv, Jar file - twitter-sentiment-analysis_2.11-0.1.jar(or your downloaded jar file) in the created S3 bucket.( If faced any errors, make sure to give public access to the files).
2) Create an EC2 keypair and then create an AWS Cluster in EMR. Enter valid name for the cluster and the keypair created in EC2 with other options as default.
3) Make sure your EC2 security group has allowed the inbound and outbound rules for all the traffic.
4) Click on the Steps tab -> ADD step in the cluster. Type Step type as Spark application.
5) Enter any valid name in the Name text field. Select Deploy mode as client.
6) Enter Spark submit options as  --class "your_scala_class_name".
7) Application location is the jar file location in the S3 bucket such as "s3://bigdata-assignment-2/twitter-sentiment-analysis_2.11-0.1.jar".
8) First Argument is the input path of the Tweets.csv file uploaded in the S3 bucket such as s3://bigdata-assignment-2/Tweets.csv".
9) Second Argument is the output file path where the output has to be saved in the S3 bucket such as  "s3://bigdata-assignment-2/Tweets_output.txt".
10) Enter all the Argument as each line in the Arguments text field.
11) Action on failure is continue.
12) As soon as the step is added it starts running in the cluster. Wait for the step to get completed and once completed download the output file which is generated in the S3 bucket.
13) The output csv file generated in the S3 bucket contains the Metrics of the Logistic Regression model trained and tested using the Tweets Data.
