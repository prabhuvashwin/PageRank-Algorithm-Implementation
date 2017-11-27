/// Author: Ashwin Venkatesh Prabhu
/// UNCC ID: 800960400
/// Email: avenka11@uncc.edu

package org.myorg;
import org.myorg.GraphLink.GraphLinkMap;
import org.myorg.GraphLink.GraphLinkReduce;
import org.myorg.PageRankComputation.PageRankComputationMap;
import org.myorg.PageRankComputation.PageRankComputationReduce;
import org.myorg.PageRankSorting.PageRankSortingMap;
import org.myorg.PageRankSorting.PageRankSortingReduce;
import org.myorg.PageRankSorting.PageRankValueComparator;
import org.myorg.WebPageCount.WebPageCountMap;
import org.myorg.WebPageCount.WebPageCountReduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/// PageRankDriver class runs all the other Map/Reduce jobs like
/// PageCount -> GraphLink - > PageRankComputation (10 iterations) -> PageRankSorting
public class PageRankDriver extends Configured implements Tool {

	public static final Logger LOG = Logger.getLogger(PageRankDriver.class);
	
	/// Page rang delimiter, outlink delimiter, and the damping factor defined here
	public static final String PAGERANK_DELIMITER = "-----";
	public static final String OUTLINK_DELIMITER = "#####";
	public static final double DAMPING_FACTOR = 0.85;
	
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new PageRankDriver(), args);
		System.exit(res);

	}

	@Override
	public int run(String[] args) throws Exception {
		
		/// Setting the intermediate directory path, input directory path, output directory path
		String intermediateDirectory = "intermediate_directory";
		Path intermediatePath = new Path(intermediateDirectory);
		String inputDirectory = args[0];
		Path outputDirectory = new Path(args[1]);
		
		Configuration initialConf = new Configuration();
		
		/// Delete the output and intermediate directories if present
		FileSystem fs = FileSystem.get(initialConf);
		
		try {
			if (fs.exists(outputDirectory))
				fs.delete(outputDirectory, true);
			
			if (fs.exists(intermediatePath))
				fs.delete(intermediatePath, true);
			
			fs.mkdirs(intermediatePath);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		/// Running the page count class MapReduce job to get the web page count for each web page
		/// The output for this is stored in ../intermediate_directory/page_count
		System.out.println("JOB: PAGE COUNT ---- STATUS: STARTED");
		Path pageCount = new Path(intermediatePath, "page_count");
		System.out.println(pageCount.toString());
		Job pageCountJob = Job.getInstance(initialConf, "PageCount");
		pageCountJob.setJarByClass(this.getClass());
		pageCountJob.setMapperClass(WebPageCountMap.class);
		pageCountJob.setReducerClass(WebPageCountReduce.class);
		FileInputFormat.addInputPaths(pageCountJob, inputDirectory);
		FileOutputFormat.setOutputPath(pageCountJob, pageCount);
		pageCountJob.setOutputKeyClass(Text.class);
		pageCountJob.setOutputValueClass(IntWritable.class);
		int success = pageCountJob.waitForCompletion(true) ? 0 : 1;
		System.out.println("JOB: PAGE COUNT ---- STATUS: ENDED");
		
		if (success == 0) {
			/// If the page count is successful, then create the link graph
			/// by running the GraphLink class MapReduce job
			System.out.println("JOB: GRAPH LINK ---- STATUS: STARTED");
			Job graphLinkJob = Job.getInstance(getConf(), "GraphLink");
			Configuration graphLinkConf = graphLinkJob.getConfiguration();
			Path graph_link = new Path(intermediatePath, "graph_link");
			System.out.println(graph_link.toString());
			int noOfPages = 0;
			
			/// The number of web pages in the input corpus is calculated by reading the
			/// output file for web page count MapReduce job "part-r-00000", present in
			/// intermediate_directory/page_count/part-r-00000
			try {
				String l = "";
				FileSystem fs1 = FileSystem.get(graphLinkConf);
				Path tempPath = new Path(pageCount, "part-r-00000");
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs1.open(tempPath)));
				while ((l = bufferedReader.readLine()) != null) {
					if (l.trim().length() > 0) {
						String[] parts = l.split("\\s+");
						noOfPages += Integer.parseInt(parts[1].trim());
					}
				}
				bufferedReader.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			/// Number of pages, outlink delimiter, pagerank delimiter is passed to
			/// the GraphLink MapReduce job
			graphLinkConf.set("noOfPages", String.valueOf(noOfPages));
			graphLinkConf.set("OUTLINK_DELIMITER", OUTLINK_DELIMITER);
			graphLinkConf.set("PAGERANK_DELIMITER", PAGERANK_DELIMITER);
			graphLinkJob.setJarByClass(this.getClass());
			graphLinkJob.setMapperClass(GraphLinkMap.class);
			graphLinkJob.setReducerClass(GraphLinkReduce.class);
			FileInputFormat.addInputPaths(graphLinkJob, inputDirectory);
			FileOutputFormat.setOutputPath(graphLinkJob,  graph_link);
			graphLinkJob.setOutputKeyClass(Text.class);
			graphLinkJob.setOutputValueClass(Text.class);
			graphLinkJob.setMapOutputKeyClass(Text.class);
			graphLinkJob.setMapOutputValueClass(Text.class);
			
			success = graphLinkJob.waitForCompletion(true) ? 0 : 1;
			System.out.println("JOB: GRAPH LINK ---- STATUS: ENDED");
			
			Path pageRankComputationInput = graph_link;
			if (success == 0) {
				
				/// If the link graph is generated, then the page rank computation is started
				/// 10 iterations are run here
				/// Outlink delimiter, page rank delimiter, and damping factor is passed to
				/// the mapreduce job
				System.out.println("JOB: PAGE RANK COMPUTATION ---- STATUS: STARTED");
				for (int i = 0; i < 10; i++) {
					System.out.println("JOB: PAGE RANK COMPUTATION ---- ITERATION: " + (i + 1) + " STATUS: STARTED");
					Job pageRankComputationJob = Job.getInstance(getConf(), "PageRankComputation");
					Configuration pageRankComputationConf = pageRankComputationJob.getConfiguration();
					pageRankComputationConf.set("OUTLINK_DELIMITER", OUTLINK_DELIMITER);
					pageRankComputationConf.set("PAGERANK_DELIMITER", PAGERANK_DELIMITER);
					pageRankComputationConf.setDouble("DAMPING_FACTOR", DAMPING_FACTOR);
					pageRankComputationJob.setJarByClass(this.getClass());
					pageRankComputationJob.setMapperClass(PageRankComputationMap.class);
					pageRankComputationJob.setReducerClass(PageRankComputationReduce.class);
					
					Path intermediatePath1 = new Path(intermediatePath, "iteration_" + (i + 1));
					System.out.println(intermediatePath1.toString());
					
					/// It is important to set this FileInputFormat and FileOutputFormat here,
					/// since the input of current iteration is the output of previous iteration,
					/// which is programmatically generated.
					/// An Exception occurs if this is not set
					FileInputFormat.addInputPath(pageRankComputationJob, pageRankComputationInput);
					FileOutputFormat.setOutputPath(pageRankComputationJob, intermediatePath1);
					
					pageRankComputationJob.setMapOutputKeyClass(Text.class);
					pageRankComputationJob.setMapOutputValueClass(Text.class);
					pageRankComputationJob.setInputFormatClass(KeyValueTextInputFormat.class);
					pageRankComputationJob.setOutputFormatClass(TextOutputFormat.class);
					pageRankComputationJob.setOutputKeyClass(Text.class);
					pageRankComputationJob.setOutputValueClass(Text.class);
					
					success = pageRankComputationJob.waitForCompletion(true) ? 0 : 1;
					
					/// current iteration output is set as input for the next iteration here
					pageRankComputationInput = intermediatePath1;
					
					System.out.println("JOB: PAGE RANK COMPUTATION ---- ITERATION: " + (i + 1) + " STATUS: ENDED");
				}
				System.out.println("JOB: PAGE RANK COMPUTATION ---- STATUS: ENDED");
			}
			
			if (success == 0) {
				
				System.out.println("JOB: PAGE RANK SORTING ---- STATUS: STARTED");
				
				/// Page rank delimiter is passed to the PageRankSorting class
				Job pageRankSortingJob = Job.getInstance(getConf(), "PageRankSorting");
				Configuration pageRankSortingConf = pageRankSortingJob.getConfiguration();
				pageRankSortingConf.set("PAGERANK_DELIMITER", PAGERANK_DELIMITER);
				pageRankSortingJob.setJarByClass(this.getClass());
				pageRankSortingJob.setMapperClass(PageRankSortingMap.class);
				pageRankSortingJob.setReducerClass(PageRankSortingReduce.class);
				
				/// It is important to set this FileInputFormat and FileOutputFormat here,
				/// since the input of current iteration is the output of previous iteration,
				/// which is programmatically generated.
				/// An Exception occurs if this is not set
				System.out.println(pageRankComputationInput.toString());
				FileInputFormat.addInputPath(pageRankSortingJob, pageRankComputationInput);
				FileOutputFormat.setOutputPath(pageRankSortingJob, outputDirectory);
				
				/// Number of reducers is set to 1
				pageRankSortingJob.setNumReduceTasks(1);
				
				pageRankSortingJob.setSortComparatorClass(PageRankValueComparator.class);
				pageRankSortingJob.setMapOutputKeyClass(DoubleWritable.class);
				pageRankSortingJob.setMapOutputValueClass(Text.class);
				pageRankSortingJob.setInputFormatClass(KeyValueTextInputFormat.class);
				pageRankSortingJob.setOutputFormatClass(TextOutputFormat.class);
				pageRankSortingJob.setOutputKeyClass(Text.class);
				pageRankSortingJob.setOutputValueClass(DoubleWritable.class);
				
				success = pageRankSortingJob.waitForCompletion(true) ? 0 : 1;
				
				/// If sorting is successful then CLEAN UP task is underway deleting all the directories
				/// created under intermediate path
				if (success == 0) {
					FileSystem fs2 = FileSystem.get(pageRankSortingConf);
					try {
						if (fs2.exists(intermediatePath))
							fs2.delete(intermediatePath, true);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
				System.out.println("JOB: PAGE RANK SORTING ---- STATUS: ENDED");
			}
		}
		
		return success;
	}

}
