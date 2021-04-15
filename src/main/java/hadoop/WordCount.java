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

	public static class TotalCountMapper extends Mapper<Object, Text, IntWritable, Text>{

		public void map(Object key, Text input, Context context) throws IOException, InterruptedException {
			String info = "";
			String value = input.toString();
			
			try {
				String splitter = "\t";
				info += "splitter: " + splitter + ", ";
				
				String[] text_count_pair = value.split(splitter);
				
				info += "size: " + text_count_pair.length + ", ";
				
				String text = text_count_pair[0];
				int count = Integer.parseInt(text_count_pair[1]);
				IntWritable i = new IntWritable(count);
				Text t = new Text(text);
				
				info += "i: " + i + ", t: " + t.toString();
				context.write(i, t);
			} catch(ArrayIndexOutOfBoundsException e) {
				throw new ArrayIndexOutOfBoundsException("[" + value + "] | " + info + " | " + e.toString());
			} catch(java.io.IOException e) {
				throw new IOException("[" + value + "] | " + info + " | " + e.toString());
			} catch(Throwable e) {
				throw new InterruptedException("[" + value + "] | " + info + " | " + e.toString());
			}
		}
	}
	
	public static class DecreasingComparator extends WritableComparator {
		
	    protected DecreasingComparator() {
	        super(IntWritable.class, true);
	    }

	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	    	IntWritable k1 = (IntWritable) w1;
	    	IntWritable k2 = (IntWritable) w2;          
	        return k1.compareTo(k2) * -1;
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
	
	private static Job createSortByValueJob(Configuration conf, String input, String output) throws IOException {
		Job job = Job.getInstance(conf, "sort by count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TotalCountMapper.class);
		job.setSortComparatorClass(DecreasingComparator.class);
		
		//TODO: Add combiner - each mapper has its own combiner, which is called after the mapper function, but before the reducer
		//TODO: Use Reducer as the combiner here ? Maybe not
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
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
			
			boolean second_job_ok = true;
			if(run_second_job) {
				// Let's then run another job that sorts the values in descending order by their values
				Job sort_by_value_job = createSortByValueJob(conf, intermediary_output_1, intermediary_output_2);
				
				second_job_ok = sort_by_value_job.waitForCompletion(true);
			}
			
			if(second_job_ok == false) {
				System.exit(1);
			} else {
				System.exit(0);
			}
			
			//int limit = findLimit(conf, intermediary_output_2);
			//conf.set("limit", "" + limit);
			
			//Job filter_top_5 = createFilterTop5(conf, intermediary_output_2, final_output);
			
			//boolean filter_job_ok = filter_top_5.waitForCompletion(true);
			
			//System.exit(filter_job_ok ? 0 : 1);
		} else {
			System.exit(1);
		}
	}
}
