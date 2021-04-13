package main.java.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
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
	public static class TotalCountMapper extends Mapper<Object, Text, IntWritable, Text>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String[] text_count_pair = value.toString().split(","); //No idea what value we get here ...
				String text = text_count_pair[0];
				int count = Integer.parseInt(text_count_pair[1].trim().strip());
				IntWritable i = new IntWritable(count);
				Text t = new Text(text);
				context.write(i, t);
			} catch(ArrayIndexOutOfBoundsException e) {
				throw new ArrayIndexOutOfBoundsException(value.toString());
			}
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
	public static class TextMapper extends Mapper<Object, Text, Text, Text>{

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, new Text("1"));
			}
		}
	}

	/**
	 * 
	 * @author MiroEklund
	 *
	 */
	public static class CountReducer extends Reducer<Text,IntWritable,Text,Text> {
		
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (Text val : values) {
				sum += Integer.parseInt(val.toString());
			}
			result.set(sum);
			context.write(key, new Text(key + "," + result.toString()));
		}
	}
	
	/**
	 * Takes as input the TotalCountMapper's (IntWritable:Text) tuples and sorts by the key IntWritable
	 * @author MiroEklund
	 *
	 */
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
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
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
		String intermediary_output;
		String final_output;
		
		if(args.length == 3) {
			input = args[0];
			intermediary_output = args[1];
			final_output = args[2];
		} else {
			input = args[1];
			intermediary_output = args[2];
			final_output = args[3];
		}
		
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(intermediary_output), true); // Delete the intermediary output directory
		fs.delete(new Path(final_output), true); 		// Clear the final output directory before adding data to it
		
		
		Configuration conf = new Configuration();
		
		// Let's first run a MapReduce job that just counts the words. Key: word, Value: count
		Job word_count_job = createWordCountJob(conf, input, intermediary_output);
		
		boolean first_job_ok = word_count_job.waitForCompletion(true);
		if(first_job_ok) {
			
			// Let's then run another job that sorts the values in descending order by their values
			Job sort_by_value_job = createSortByValueJob(conf, intermediary_output, final_output);
			
			boolean second_job_ok = sort_by_value_job.waitForCompletion(true);
			
			fs.delete(new Path(intermediary_output), true); //Delete the intermediary output directory
			
			System.exit(second_job_ok ? 0 : 1);
		} else {
			System.exit(1);
		}
	}
}
