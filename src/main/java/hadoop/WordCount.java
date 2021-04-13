package main.java.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	/**
	 * Changes (text:count) -> (count:Iterator<Text>) from the point-of-view of the reducer
	 * @author MiroEklund
	 *
	 */
	public static class TotalCountMapper extends Mapper<Text, IntWritable, IntWritable, Text>{

		public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
			context.write(value, key);
		}
	}

	/**
	 * Changes (TextCountPair:value) -> (Text:IntWritable), but uses TextCountPair for sorting
	 * @author MiroEklund
	 *
	 */
	public static class TotalCountReducer extends Reducer<IntWritable,Text,Text,IntWritable> {
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text t : values) {
				context.write(t, key);
			}
		}
	}
	
	/**
	 * 
	 * @author MiroEklund
	 *
	 */
	public static class TextMapper extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	/**
	 * 
	 * @author MiroEklund
	 *
	 */
	public static class CountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
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
	private static Job createSortByValueJob(Configuration conf, String input, String output) throws IOException {
		Job job = Job.getInstance(conf, "sort by count and filter only top 100");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TotalCountMapper.class);
		job.setReducerClass(TotalCountReducer.class);
		
		// We should use a decreasing order, based on the count key
		job.setSortComparatorClass(DecreasingComparator.class);
		
		// Swapped order compared to wordCountJob!
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job;
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
		Job job = Job.getInstance(conf, "perform word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TextMapper.class);
		job.setReducerClass(CountReducer.class);
		
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
		String output;
		String output2;
		
		if(args.length == 3) {
			input = args[0];
			output = args[1];
			output2 = args[2];
		} else {
			input = args[1];
			output = args[2];
			output2 = args[3];
		}
		
		Configuration conf = new Configuration();
		
		// Let's first run a MapReduce job that just counts the words. Key: word, Value: count
		Job word_count_job = createWordCountJob(conf, input, output);
		
		boolean first_job_ok = word_count_job.waitForCompletion(true);
		if(first_job_ok) {
			
			// Let's then run another job that sorts the values in descending order by their values
			Job sort_by_value_job = createSortByValueJob(conf, output, output2);
			
			boolean second_job_ok = sort_by_value_job.waitForCompletion(true);
			
			System.exit(second_job_ok ? 0 : 1);
		} else {
			System.exit(1);
		}
	}
}
