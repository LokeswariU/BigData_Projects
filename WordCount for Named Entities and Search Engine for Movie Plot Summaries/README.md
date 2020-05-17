1. Login to databricks.com 
2. Create a cluster with the following python libraries(PyPi) installed on the cluster:
	- JohnSnowLabs:spark-nlp:2.5.0
3. Create a new notebook by selecting import notebook from URL =>
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4118661854845849/1508370314715707/8152562404163685/latest.html
4. Taken a large text file of the story Tale of Two Cities from Gutenberg website : https://www.gutenberg.org/ebooks/98. You can choose any other books as plain text from the Gutenberg website : https://www.gutenberg.org/
5. The code is written using spark in scala technology. Using Map Reduce technique in scala, the named entities are extracted for the whole document.
6. Using the Pre-trained pipeline of the Johnsnow NLP aprk library, the document is converted to an array of entities(words) without stopwords.
7. This array of entities is mapped and reduced to a key value pair with each word as Key and their count of occurences as their value.
