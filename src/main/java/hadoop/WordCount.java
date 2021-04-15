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

	private static final double price_per_request = 0.00001; //euros
	private static final double price_per_gb_data = 0.0008; //euros
	
	/**
	 * 
	 * @author MiroEklund
	 *
	 */
	public static class BytesMapper extends Mapper<Object, Text, Text, IntWritable>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String info = "";
			LineSplitter s = null;
			try {
				info += "value: [" + value.toString() + "]";
				
				s = new LineSplitter(value.toString());
				s.parse();
				
				if(s.subnet != null) {
					context.write(new Text(s.subnet), new IntWritable(1));
				}
				
			} catch(Throwable e) {
				if(s != null) {
					info += ", ip: [" + s.ip + "]";
				}
				throw new IOException(info + e.getMessage());
			}
		}
	}

	public static class PriceReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			context.write(key, new IntWritable(sum));
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
		Job job = Job.getInstance(conf, "calculate cdn");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(BytesMapper.class);
		job.setCombinerClass(PriceReducer.class);
		job.setReducerClass(PriceReducer.class);
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job;
	}
	
	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		String input;
		String final_output;
		
		if(args.length == 2) {
			input = args[0];
			final_output = args[1];
		} else {
			input = args[1];
			final_output = args[2];
		}
		
		Configuration conf = new Configuration();
		
		Job word_count_job = createWordCountJob(conf, input, final_output);
		boolean first_job_ok = word_count_job.waitForCompletion(true);
		
		if(first_job_ok) {
			System.exit(0);
		} else {
			System.exit(1);
		}
	}
}
