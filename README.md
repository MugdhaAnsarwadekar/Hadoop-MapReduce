
<h3>
Introduction:
</h3>
This project has word count and word co-occurence implementation using Hadoop MapReduce as a part of CSE 587 Data Intensive Computing course and has three basic parts.<br>

<h5>
Part 1: 
</h5>
It has basic implementation of word count on twitter data. The input for this part is tweets collected using Twitter API. Tweets were cleaned by removing punctuation, links and stopwords with Python code. The output contains count of each word in the input file.<br>

<h5>
Part 2:
</h5>
It has implementation of word co-occurrence using Pairs and Strips methods on twitter data and One line of the input is considered as the neighborhood for co-occurence. The input is preprocessed text file with tweets without links and stopwords. The output contains count of each occurrence of unorderd pair of input words.<br>

<h5>
Part 3:
</h5>
It has implementation of word count with lemmatization on Classical Latin text data provided by researchers in the Classics department at University at Buffalo. Input for this part contains classical texts and lemmatization file to convert words from one form to a standard or normal form. <br>
For each word in the input, the mapper normalizes the word and gets its lemma if present. The mapper then emits location of the word for each lemma of the word. For each lemma, reducer combines its locations and emits them. <br>
<br>
The output of the mapper is in the following format where location is <docname, [chapter,line]>: <br>
if lemma is present in file:<br>
lemma location<br>
else if lemma is not present in the file:<br>
word location<br>
<br>
The output of the reducer is in the following format:<br>
lemma location1, location2, location3,...<br>
where each locaiton is the location of the lemma in all files<br>
