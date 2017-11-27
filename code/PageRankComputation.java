/// Author: Ashwin Venkatesh Prabhu
/// UNCC ID: 800960400
/// Email: avenka11@uncc.edu

package org.myorg;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/// PageRankComputation class has a Mapper and a Reducer class. This is the component that 
/// will be run in the main loop. The output of this phase is directly readable as the input 
/// of the same step, so that we can run it multiple times. The loop is run 10 times in this program
public class PageRankComputation {
	
	/// This Mapper class is used to calculate page rank for each web page. The "page" passed here is
	/// the web page, and the "prOutlinks" passed here is the string having initial page rank for the
	/// web page along with all the outlinks
	public static class PageRankComputationMap extends Mapper<Text, Text, Text, Text> {
		
		public void map (Text page, Text prOutlinks, Context context) throws IOException, InterruptedException {
			
			/// Outlink delimiter and Pagerank delimiter are defined and passed from the driver class
			String OUTLINK_DELIMITER = context.getConfiguration().get("OUTLINK_DELIMITER");
			String PAGERANK_DELIMITER = context.getConfiguration().get("PAGERANK_DELIMITER");
			
			String pageRankValue = "";
			String[] outlinks = null;
			
			/// prOutlinks is split using the Page rank delimiter
			String[] tempSplit = prOutlinks.toString().split(PAGERANK_DELIMITER);
			
			/// Check for string array length to avoid ArraysOutofBound exception
			/// 0th position sholds the page rank value
			if (tempSplit.length > 0)
				pageRankValue = tempSplit[0];
			
			/// 1st position holds the string with all outlinks joined by the outlink delimiter
			/// "#####", which is split on the delimiter and stored in the variable "outlinks"
			if (tempSplit.length > 1)
				outlinks = tempSplit[1].split(OUTLINK_DELIMITER);
			
			/// If the outlinks are present, then proceed
			if (outlinks != null && outlinks.length > 0) {
				
				/// Get the number of outlinks
				double numberOfOutlinks = (double) outlinks.length;
				
				/// If the page rank is available
				if (!pageRankValue.isEmpty()) {
					
					/// Calculate the new page rank, which is (page rank value / number of outlinks)
					double newPageRankValue = Double.parseDouble(pageRankValue) / numberOfOutlinks;
					for (String outlink : outlinks) {
						/// Iterate through all outlinks. If the outlinks is not empty, then
						/// combine the outlink with the new page rank value
						if (!outlink.isEmpty()) {
							context.write(new Text(outlink), new Text(Double.toString(newPageRankValue)));
						}
					}
				}
			}
			
			/// Append the web page info and the initial page rank along with the outlinks
			/// to the context
			if (!page.toString().isEmpty() && !prOutlinks.toString().isEmpty())
				context.write(page, prOutlinks);
		}
	}
	
	/// Reducer class will calculate the pagerank for the web page for the data passed from the
	/// Mapper by including the damping factor, d = 0.85
	/// The iterable passed to this reduce function consists of a list of values which has one
	/// (url / page rank outlinks list) and other (outlink / new page rank value)
	public static class PageRankComputationReduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce (Text page, Iterable<Text> it, Context context) throws IOException, InterruptedException {
			
			/// Page rank delimiter and damping factor is passed from the driver class
			String PAGERANK_DELIMITER = context.getConfiguration().get("PAGERANK_DELIMITER");
			Double DAMPING_FACTOR = Double.parseDouble(context.getConfiguration().get("DAMPING_FACTOR"));
			
			/// flag is a boolean variable initially set to false, but set to true when the 
			/// (url / page rank outlinks list) is found
			boolean flag = false;
			String outlinkList = "";
			
			double newPageRank = 0;
			
			/// One line of the mapper output consist of web page info and old pagerank and 
			/// outlink list. The rest of the lines consist of page rank and outlinks. The
			/// following code separates out these values
			for (Text value :  it) {
				String str = value.toString();
				
				if (str.contains(PAGERANK_DELIMITER)) {
					/// If the string contains page rank delimiter, then separate the old page rank
					/// from the outlinks list
					String[] graphLinkList = str.split(PAGERANK_DELIMITER);
					
					if (graphLinkList.length == 2) outlinkList = graphLinkList[1];
					
					/// setting the flag to true since the outlinks list is found
					flag = true;
				} else {
					/// add the new page rank value calculated for the outlink to the "newPageRank"
					/// variable
					newPageRank += Double.parseDouble(str);
				}
			}
			
			/// page rank after damping for the web page is calculated using the formula,
			/// pr = (1 - d) + d * (new page rank value)
			double pageRankAfterDamping = (1 - DAMPING_FACTOR) + DAMPING_FACTOR * newPageRank;
			
			/// web page info, along with page rank value after damping followed by page rank delimiter and
			/// outlink list is written to the file. The output will be written to,
			/// ../intermediate_directory/iteration_<number>/. This directory is passed as input to the
			/// next iteration
			if (flag && !page.toString().isEmpty())
				context.write(page, new Text(String.valueOf(pageRankAfterDamping) + PAGERANK_DELIMITER + outlinkList));
		}
	}
}
