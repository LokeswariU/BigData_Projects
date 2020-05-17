How to run the Project 

Executing the project locally

1.	Install Java 8 and set all the path variables
2.	Install python 3.8 and set all the path variables
3.	Install Spark and set all the path variables
4.	Run the given python file
In case the above method did not work out it might be due to the library version incompatibilities. Please use databricks notebook to execute the code.

Follow these steps to run the project on databricks.

1. Login to databricks.com 
2. Create a cluster with the following python libraries(PyPi) installed on the cluster:
	- s3fs
	- boto3
	- sklearn
	- nltk
	- networkx	
- pandas
	- bs4
	- requests
2. create a new notebook by selecting import notebook 	=> from URL =>
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4987911583861567/4345931203554285/3511878718732474/latest.html
3. once the cluster is up and running attach the notebook to the cluster
4. select run all commands 
5. You will find the plots created in the notebook and output files (analysis.csv and output.csv) in the S3 bucket after the commands are executed
6. In order to access the output files (analysis.csv and output.csv) paste the 2 urls of cmd 25  in the browser seperatly and press Enter. 
Files will be automatically downloaded once the URL is pasted in the browser and enter is pressed.

** Note : please allow atleast 30-35 minutes for all the commands to execute as the data file is large.

Follow these steps to run the project in AWS notebook cluster:

1.	Login to AWS and create a notebook cluster using the same steps to create a AWS cluster. Ensure that the spark option is enabled while creating the cluster.
2.	Once the cluster is started, click on the option open in Jupyter. This opens a notebook in jupyter.
3.	In the Jupyter notebook import the attached notebook Summarization.pynb file.
4.	After importing run all the cells in the notebook. This will take approximately 30-40 minutes to completely run. 
5.	Since the data is parsed from many URL’s of the dataset, it takes time to parse each URL and save the retrieved data into a dataframe. This dataframe along with the retrieved data and their respective reference summary is saved in AWS S3 bucket. This can be retrieved or used later.
6.	The output dataframe with the generated summary and the reference summary is also saved in the AWS S3 bucket. Code is written for saving these files.
7.	The analysis dataframe with the Precision, Recall and F-Statistics is also saved in the AWS S3 bucket. 


