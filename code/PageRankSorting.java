/// Author: Ashwin Venkatesh Prabhu
/// UNCC ID: 800960400
/// Email: avenka11@uncc.edu

package org.myorg;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/// PageRankSorting consists of a Mapper and a Reducer class. This class sorts the calculated
/// page ranks in descending order using a comparator
public class PageRankSorting {
	
	/// This Mapper class gets the page info and the page rank of the page along with the outlinks
	/// separated by the page rank delimiter
	public static class PageRankSortingMap extends Mapper<Text, Text, DoubleWritable, Text> {
		
		public void map (Text page, Text prOutlinks, Context context) throws IOException, InterruptedException {
			
			/// page rank delimiter is passed from the driver class
			String PAGERANK_DELIMITER = context.getConfiguration().get("PAGERANK_DELIMITER");
			
			/// From the string consisting of the page rank value and the outlinks list,
			/// we extract the page rank value
			String[] temp = prOutlinks.toString().split(PAGERANK_DELIMITER);
			
			/// we attache the page rank value and the page info to the context
			if (temp.length > 0) {
				context.write(new DoubleWritable(Double.valueOf(temp[0])), page);
			}
		}
	}
	
	/// Reducer class creates the key value pair of page and its corresponding page rank value.
	/// These values will be in the descending sorted order
	public static class PageRankSortingReduce extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		
		public void reduce(DoubleWritable pagerank, Iterable<Text> pages, Context context) throws IOException, InterruptedException {
			
			for (Text page : pages) {
				context.write(page, pagerank);
			}
		}
	}
	
	/// Comparator class referenced from the code below,
	/// Reference: https://vangjee.wordpress.com/2012/03/30/implementing-rawcomparator-will-speed-up-your-hadoop-mapreduce-mr-jobs-2/
	public static class PageRankValueComparator extends WritableComparator {
		
		public PageRankValueComparator() {
			super();
		}
		
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			
			double rank1 = WritableComparator.readDouble(b1, s1);
			double rank2 = WritableComparator.readDouble(b2, s2);
			
			return (rank1 > rank2) ? -1 : ((rank1 < rank2) ? 1 : 0);
		}
	}
}
