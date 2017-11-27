/// Author: Ashwin Venkatesh Prabhu
/// UNCC ID: 800960400
/// Email: avenka11@uncc.edu

package org.myorg;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/// The GraphLink class has a Mapper class and a Reducer class which take the input web pages from the
/// input location provided by the user in command line.
public class GraphLink {
	
	/// Mapper class to calculate the link graph of all the web pages present in the input
	public static class GraphLinkMap extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			
			/// Total number of pages, outlink delimeter, and pagerank delimiter are defined in the
			/// pagerank driver class and passed through the configuration object
			String numOfPages = context.getConfiguration().get("noOfPages");
			String OUTLINK_DELIMITER = context.getConfiguration().get("OUTLINK_DELIMITER");
			String PAGERANK_DELIMITER = context.getConfiguration().get("PAGERANK_DELIMITER");
			
			/// The initial page rank for all web pages is set to 1 / number of pages
			double initialPageRank = 1 / Double.parseDouble(numOfPages);
			
			String webPage = lineText.toString().trim();
			
			try {
				
				/// A web page is represented by the <title> tags which consist of the web page name
				/// and the <text> tag which consist of all the outlinks from this web page. The code
				/// reference to achieve this is given below,
				/// Reference: https://examples.javacodegeeks.com/core-java/util/regex/matcher-group-example/
				/// The below code extracts the title info from between <title> tags
				Pattern titlePattern = Pattern.compile("<title>(.*?)</title>");
				Matcher titleMatch = titlePattern.matcher(webPage);
				
				String urlTitle = "";
				while (titleMatch.find()) {
					urlTitle = titleMatch.group(1);
				}
				
				/// A string is defined which holds the initial page rank (1 / N) followed by the
				/// page rank delimiter "-----". Eg: 0.000015-----
				String pageRankOutlinks = Double.toString(initialPageRank) + PAGERANK_DELIMITER;
				
				/// The below code extracts the outlinks from in between the <text> tags
				Pattern textPattern = Pattern.compile("<text(.*?)</text>");
				Matcher textMatch = textPattern.matcher(webPage);
				
				while (textMatch.find()) {
					String temp = textMatch.group(1);
					
					/// All the outlinks are represented as [[outlink]], and the same is extracted
					/// using the below pattern match
					Pattern outlinkPattern = Pattern.compile("\\[\\[(.*?)\\]\\]");
					Matcher outlinkMatch = outlinkPattern.matcher(temp);
					
					while (outlinkMatch.find()) {
						/// "[[" and "]]" are eliminated and other the outlink info is kept
						String outlinkURL = outlinkMatch.group(1).replace("[[",  "").replace("]]", "");
						
						/// the outlinks are attached to the initial page rank
						/// Eg: 0.000015-----outlink1#####outlink2#####outlink3#####....
						pageRankOutlinks += outlinkURL + OUTLINK_DELIMITER;
					}
				}
				
				/// If the web page title info is available and page rank outlinks are present,
				/// then write it
				if (!urlTitle.isEmpty() && !pageRankOutlinks.isEmpty())
					context.write(new Text(urlTitle), new Text(pageRankOutlinks));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/// Reducer class for creating graph links. Output for this file is stored in the path
	/// ../intermediate_directory/graph_link. This output will eventually be used by the driver class
	/// for page rank computation.
	public static class GraphLinkReduce extends Reducer<Text, Text, Text, Text> {
		
		/// This reduce function takes the web page info and all the outlinks related to it
		/// and writes it to a file in the intermediate directory
		public void reduce(Text url, Iterable<Text> outlinks, Context context) throws IOException, InterruptedException {
			for (Text outlink : outlinks) {
				context.write(url, outlink);
			}
		}
	}

}
