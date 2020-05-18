1.**WordCount for Named Entities** - Taken a large text file dataset from the Gutenberg Website and find their named entities and their count for each word. From the output we can retrieve the top frequently occurring words.


**WordCount for Named Entities**

1. Login to databricks.com 
2. Create a cluster with the following python libraries(PyPi) installed on the cluster:
	- JohnSnowLabs:spark-nlp:2.5.0
3. Create a new notebook by selecting import notebook from URL =>
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4118661854845849/1508370314715707/8152562404163685/latest.html
4. Taken a large text file of the story Tale of Two Cities from Gutenberg website : https://www.gutenberg.org/ebooks/98. You can choose any other books as plain text from the Gutenberg website : https://www.gutenberg.org/ . Download and upload the text file to the Databricks Database.
5. The code is written using spark in scala technology. Using Map Reduce technique in scala, the named entities are extracted for the whole document.
6. Using the Pre-trained pipeline of the Johnsnow NLP aprk library, the document is converted to an array of entities(words) without stopwords.
7. This array of entities is mapped and reduced to a key value pair with each word as Key and their count of occurences as their value in descending order.

**Search Engine for Movie Plot Summaries**

1. Login to databricks.com 
2. Create a new notebook by selecting import notebook from URL =>
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4118661854845849/1934211613559818/8152562404163685/latest.html
3. Download the Movies Plot summary available in the Carnegie Movie Summary Corpus site http://www.cs.cmu.edu/~ark/personas/data/MovieSummaries.tar.gz
4. Remove all the stopwords using stopwords.txt file, convert to lowercase and split the document to individual words using mapreduce function and clean the data without null values.
5. Give some search terms like movie names or movie genre to be searched in a text file and pass as input.
6. Written a function for cosine similarity and TF-IDF to find the document and word similarity.
7. If the search term is greater than one, cosine similarity is used and if the search term is less than one TF-IDF is used to find the Document-Term similarity and retrieve the most frequent movies related to those search terms.
5. 
