**Dataset:**  The Dataset is obtained from the link https://snap.stanford.edu/data/web-Google.html. Nodes represent web pages and directed edges represent hyperlinks between them. The data was released in 2002 by Google.

**Implementation:**
1. Download the data web-Google.txt.gz from the "https://snap.stanford.edu/data/web-Google.html" . Upload the file to your S3 AWS Bucket.
This can also be retrieved from the public S3 bucket path - "https://assignment-3-bigdata.s3.amazonaws.com/web-Google.txt"
2. Create a S3 bucket in AWS account. Upload the text file to the S3 bucket.
3. Upload the jar file in the same S3 bucket.
4. Create a cluster in the EMR. Start the cluster.
5. Add step to the cluster giving the path of the input file, jar file and to be created output files in the S3 bucket as parameters to the step.
6. The output path specified in the S3 bucket creates 5 different text files for each query output.
7. After adding the step, the cluster will start running. After it is completed, the output files can be downloaded from the S3 bucket.
