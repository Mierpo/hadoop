package main.java.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * What we have so far: Second MapReducer procudes a (count : text) list in descending order.
 * Now we just need to somehow grab the 100 first lines from this, and send those as results to the final output bucket...
 * 
 * @author MiroEklund
 *
 */

public class WordCount {

	/**
	 * 
	 * @author MiroEklund
	 *
	 */
	public static class WordLength3or5Mapper extends Mapper<Object, Text, Text, IntWritable>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String v = itr.nextToken();
				if(v.length() == 3) {
					context.write(new Text("length_five"), new IntWritable(1)); //Only add an occurence for the words that have 3 or 5 length
				} else if(v.length() == 5) {
					context.write(new Text("length_three"), new IntWritable(1)); //Only add an occurence for the words that have 3 or 5 length
				}
			}
		}
	}

	/**
	 * 
	 * @author MiroEklund
	 *
	 */
	public static class WordLength3or5Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		
		private IntWritable result = new IntWritable();

		// Gets two keys: length_five and length_three.
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	/**
	 * 
	 * @param conf
	 * @param input
	 * @param output
	 * @return
	 * @throws IOException
	 */
	private static Job createWordCountJob(Configuration conf, String input, String output) throws IOException {
		Job job = Job.getInstance(conf, "calculate word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordLength3or5Mapper.class);
		job.setReducerClass(WordLength3or5Reducer.class);
		job.setCombinerClass(WordLength3or5Reducer.class); //This is the only line that changed between "Application 1 and 2"
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job;
	}
	
	//Use:
	//s3://aaucloudcomputing20/fiwiki-latest-pages-articles_preprocessed.txt
	
	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		String input;
		String intermediary_output_1;
		String intermediary_output_2;
		String final_output;
		
		// For saving time when debugging jobs - no need to run job 1, if problem is in job 2, etc.
		boolean run_first_job = true;
		boolean run_second_job = true;
		
		if(args.length == 2) {
			input = args[0];
			final_output = args[1];
			intermediary_output_1 = final_output + "-1-temp";
			intermediary_output_2 = final_output + "-2-temp";
		} else {
			input = args[1];
			final_output = args[2];
			intermediary_output_1 = final_output + "-1-temp";
			intermediary_output_2 = final_output + "-2-temp";
		}
		
		Configuration conf = new Configuration();
		
		// Let's first run a MapReduce job that just counts the words. Key: word, Value: count
		boolean first_job_ok = true;
		
		if(run_first_job) {
			Job word_count_job = createWordCountJob(conf, input, intermediary_output_1);
			first_job_ok = word_count_job.waitForCompletion(true);
		}
		if(first_job_ok) {
			System.exit(0);
		} else {
			System.exit(1);
		}
	}
}
