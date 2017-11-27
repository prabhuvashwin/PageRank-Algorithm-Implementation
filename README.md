Page Rank Algorithm Implementation
-----------------------------------------------------------------------------------------

This is a read me document for the assignment 3 and explains all the files involved.

The input files used for this dataset is the Wiki-Micro Corpus.

While running the program, two directories are generated - one is an output directory, OUT_DIR, which will host the final output, and the other is an intermediate directory, INT_DIR, which will host the outputs of WebPageCount, GraphLink, and PageRankComputation. The input is present in INPUT_DIR.

The program works in the following way.
1) PageRankDriver.java drives the program. It is responsible for calling the required class in the sequential order, and setting the right input and output formats for different classes. It also takes care of the cleaning up the intermediate directories which were generated during the program execution.

2) WebPageCount.java is the first MapReduce job called. It is responsible for taking into account all the pages and its count. This class takes the input from INPUT_DIR. The output of this MapReduce job will be stored at INT_DIR/page_count. PageRankDriver uses the output of this file to calculate "N", which is the number of web pages present in the corpus. This value is used for page rank calculation.

3) Next MapReduce job being called is GraphLink.java. GraphLink uses the INPUT_DIR as input and "N" calculated using the output of WebPageCount to generate a link graph of all the web pages. The output of this MapReduce job will be stored at INT_DIR/graph_link

4) Next MapReduce job being called is PageRankComputation.java. This job will be called 10 times sequentially. The initial input of this job is INT_DIR/graph_link. The output of 1st iteration is used as the input of the 2nd iteration, and so on. The outputs of the respective iterations will be stored at INT_DIR/iteration_<iteration_id>.

5) The next MapReduce job will be PageRankSorting.java. This job uses the out of the last iteration of PageRankComputation stored at INT_DIR/iteration_10, as input. It sorts the pages on page rank in descending order using a comparator. The output of this computation is stored in OUT_DIR.

REFERENCES USED:
-----------------------------------------------------------------------------------------

1) Pattern matcher group - https://examples.javacodegeeks.com/core-java/util/regex/matcher-group-example/

2) Comparator class for sorting - https://vangjee.wordpress.com/2012/03/30/implementing-rawcomparator-will-speed-up-your-hadoop-mapreduce-mr-jobs-2/

3) http://hadoop.apache.org/core/docs/current/api/

-----------------------------------------------------------------------------------------
NOTE: I HAVE NOT RUN THIS PROGRAM ON CLUSTER AND THE BELOW INSTRUCTIONS ARE FOR CLOUDERA VM

- Copy all the source code files onto the cluster at path ~/org/myorg/
- Use "hadoop fs -copyFromLocal <source> <destination>" to copy files to hadoop. Input files need to be under /input directory
- Compile the program using "java -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* *.java -d build -Xlint
- Build the jar file using "jar -cvf page rank.jar -C build/ ."
- Run the program using "hadoop jar page rank.jar org.myorg.PageRankDriver /user/cloudera/input /user/cloudera/output"
- Copy the output to the local file system using "hadoop fs -copyToLocal /user/cloudera/output/* output"
-----------------------------------------------------------------------------------------