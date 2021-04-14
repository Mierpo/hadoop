package main.java.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
	 * Values coming into this are: 123\tFOOBAR
	 * @author MiroEklund
	 *
	 */
	public static class FilterTop100Mapper extends Mapper<Object, Text, IntWritable, Text>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String info = "";
			try {
				String limit = context.getConfiguration().get("limit");
				int limit_count = Integer.parseInt(limit);
				
				String splitter = "\t";
				if(value.toString().contains("\t")) {
					// Ok
				} else {
					splitter = ",";
				}
				
				info += "limit_count: " + limit_count + ", ";
				info += "splitter: " + splitter + ", ";
				
				String[] text_count_pair = value.toString().split(splitter); //No idea what value we get here ...
				
				info += "size: " + text_count_pair.length + ", ";
				
				String text = text_count_pair[1];
				int count = Integer.parseInt(text_count_pair[0].trim().strip());
				IntWritable i = new IntWritable(count);
				Text t = new Text(text);
				
				info += "i: " + i + ", t: " + t.toString();
				
				if(i.get() >= limit_count) {
					context.write(i, t);
				}
				
			} catch(ArrayIndexOutOfBoundsException e) {
				throw new ArrayIndexOutOfBoundsException("[" + value.toString() + "] | " + info + " | " + e.toString());
			} catch(java.io.IOException e) {
				throw new IOException("[" + value + "] | " + info + " | " + e.toString());
			} catch(Throwable e) {
				throw new InterruptedException("[" + value + "] | " + info + " | " + e.toString());
			}
		}
	}
	
	/**
	 * Values coming into this are: FOOBAR\t123
	 * Changes (text:count) -> (count:Iterator<Text>) from the point-of-view of the reducer
	 * @author MiroEklund
	 *
	 */
	public static class TotalCountMapper extends Mapper<Object, Text, IntWritable, Text>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String info = "";
			try {
				String splitter = "\t";
				if(value.toString().contains("\t")) {
					// Ok
				} else {
					splitter = ",";
				}
				
				info += "splitter: " + splitter + ", ";
				
				String[] text_count_pair = value.toString().split(splitter); //No idea what value we get here ...
				
				info += "size: " + text_count_pair.length + ", ";
				
				String text = text_count_pair[0];
				int count = Integer.parseInt(text_count_pair[1].trim().strip());
				IntWritable i = new IntWritable(count);
				Text t = new Text(text);
				
				info += "i: " + i + ", t: " + t.toString();
				context.write(i, t);
			} catch(ArrayIndexOutOfBoundsException e) {
				throw new ArrayIndexOutOfBoundsException("[" + value.toString() + "] | " + info + " | " + e.toString());
			} catch(java.io.IOException e) {
				throw new IOException("[" + value + "] | " + info + " | " + e.toString());
			}
		}
	}

	/**
	 * Changes (TextCountPair:value) -> (Text:IntWritable), but uses TextCountPair for sorting
	 * @author MiroEklund
	 *
	 */
	public static class TotalCountReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text t : values) {
				context.write(key, t);
			}
		}
	}
	
	/**
	 * 
	 * @author MiroEklund
	 *
	 */
	public static class TextMapper extends Mapper<Object, Text, Text, IntWritable>{

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, new IntWritable(1));
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
		Job job = Job.getInstance(conf, "sort by count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TotalCountMapper.class);
		job.setReducerClass(TotalCountReducer.class);
		
		// We should use a decreasing order, based on the count key
		job.setSortComparatorClass(DecreasingComparator.class);
		
		//TODO: Add combiner - each mapper has its own combiner, which is called after the mapper function, but before the reducer
		//TODO: Use Reducer as the combiner here ? Maybe not
		//TODO: Possibly add a Partitioner?
		
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
		Job job = Job.getInstance(conf, "calculate word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TextMapper.class);
		job.setReducerClass(CountReducer.class);
		
		//TODO: Add combiner - each mapper has its own combiner, which is called after the mapper function, but before the reducer
		//TODO: Use Reducer as the combiner here -> pre-calculated occurances of specific word by summing the count, just like the Reducer does.
		//TODO: Possibly add a Partitioner?
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job;
	}
	
	private static Job createFilterTop100(Configuration conf, String input, String output) throws IOException {
		Job job = Job.getInstance(conf, "calculate word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(FilterTop100Mapper.class);
		
		// We should use a decreasing order, based on the count key
		job.setSortComparatorClass(DecreasingComparator.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job;
	}
	
	private static int findLimit(FileSystem fs, String hdfs_directory) throws IllegalArgumentException, IOException {
		FileStatus[] status = fs.listStatus(new Path(hdfs_directory));
		
		int count = 0;
		for(FileStatus s : status) {
			Path file_path = s.getPath();
			FSDataInputStream fsDataInputStream = fs.open(file_path);
			BufferedReader br = null;
			
			try {
				br = new BufferedReader(new InputStreamReader(fsDataInputStream));
			    
				String l = null;
			    while((l = br.readLine())!= null){
			    	count++;
			    	if(count == 100) {
			    		//Format: 100\tText
			    		String[] values = l.split("\t");
			    		String word_count = values[0];
			    		return Integer.parseInt(word_count); // We found our answer
			    	}
			    }
			} finally {
				fsDataInputStream.close();
				if(br != null) {
					br.close();
				}
			}
		}
		
		
		/*
		FSDataInputStream fsDataInputStream = fs.open(new Path(hdfs_directory));
		
		//TODO: We have a directory, now we need to loop all files
		
		BufferedReader br = null;
		
		try {
			br = new BufferedReader(new InputStreamReader(fsDataInputStream));
		    
			int count = 0;
			
			String l = null;
		    while((l = br.readLine())!= null){
		    	count++;
		    	if(count == 100) {
		    		//Format: 100\tText
		    		String[] values = l.split("\t");
		    		String word_count = values[0];
		    		return Integer.parseInt(word_count); // We found our answer
		    	}
		    }
		} finally {
			fsDataInputStream.close();
			if(br != null) {
				br.close();
			}
		}
		*/
		
		return 0;
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
		
		if(args.length == 2) {
			input = args[0];
			final_output = args[1];
			intermediary_output_1 = final_output + "_temp1";
			intermediary_output_2 = final_output + "_temp2";
		} else {
			input = args[1];
			final_output = args[2];
			intermediary_output_1 = final_output + "_temp1";
			intermediary_output_2 = final_output + "_temp2";
		}
		
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(intermediary_output_2), true); // Delete the intermediary output directory
		fs.delete(new Path(intermediary_output_1), true); // Delete the intermediary output directory
		fs.delete(new Path(final_output), true); 		// Clear the final output directory before adding data to it
		
		Configuration conf = new Configuration();
		
		// Let's first run a MapReduce job that just counts the words. Key: word, Value: count
		Job word_count_job = createWordCountJob(conf, input, intermediary_output_1);
		
		boolean first_job_ok = word_count_job.waitForCompletion(true);
		if(first_job_ok) {
			
			// Let's then run another job that sorts the values in descending order by their values
			Job sort_by_value_job = createSortByValueJob(conf, intermediary_output_1, intermediary_output_2);
			
			boolean second_job_ok = sort_by_value_job.waitForCompletion(true);
			
			if(second_job_ok == false) {
				System.exit(1);
			}
			
			fs.delete(new Path(intermediary_output_1), true); //Delete the intermediary output directory
			
			// Read intermediary output 2 and find the count limit.
			
			int limit = findLimit(fs, intermediary_output_2);
			conf.set("limit", "" + limit);
			
			Job filter_top_100 = createFilterTop100(conf, intermediary_output_2, final_output);
			
			boolean filter_job_ok = filter_top_100.waitForCompletion(true);
			
			fs.delete(new Path(intermediary_output_2), true); //Delete the second intermediary output directory
			
			System.exit(filter_job_ok ? 0 : 1);
		} else {
			System.exit(1);
		}
	}
}
