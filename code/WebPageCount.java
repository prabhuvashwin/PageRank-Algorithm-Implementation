/// Author: Ashwin Venkatesh Prabhu
/// UNCC ID: 800960400
/// Email: avenka11@uncc.edu

package org.myorg;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/// This class assigns a value 1 to each page found in the input. In the input file, each line is considered
/// to be a web page. This class has two classes defined inside - one Mapper class and one Reducer class
public class WebPageCount {
	
	/// This is the Mapper class which hosts map function. This Mapper class reads the files from 
	/// the input directory passed by the user in the command line.
	public static class WebPageCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		public final static IntWritable one = new IntWritable(1);
		
		/// It considers each line as a web page, and if the line is not empty, then assigns a value 1 to it
		public void map(LongWritable offset, Text lineText, Context context) 
				throws IOException, InterruptedException {
			String webPage = lineText.toString();
			
			if (!webPage.isEmpty()) 
				context.write(lineText, one);
		}
	}
	
	/// This is the Reducer class which hosts the reduce function. The input received here is from the
	/// Mapper class (web page and its assigned value 1). Output for this file is stored in the path
	/// ../intermediate_directory/page_count. This output will eventually be used by the driver class
	/// to determine the total number of pages available in the input.
	public static class WebPageCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text page, Iterable<IntWritable> counts, Context context) 
				throws IOException, InterruptedException {
			int pageCount = 0;
			
			/// For every web page which appears N number of times, this reduce function gets the 
			/// value of N and stores it along with the string "PageCount:"
			for (IntWritable count : counts) {
				pageCount += count.get();
			}
			
			/// This line will store the web page count as "PageCount:	1"
			/// A dummy string ("PageCount:") is passed here in place of the actual web page
			/// This is because, when I pass the actual page here, it gave an error during the split
			/// operation. The web page is not really needed here as we only need this to obtain the value
			/// of number of pages, N.
			context.write(new Text("PageCount:"), new IntWritable(pageCount));
		}
	}
}
