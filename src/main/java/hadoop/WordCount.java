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
				int count = Integer.parseInt(text_count_pair[0]);
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

		public void map(Object key, Text input, Context context) throws IOException, InterruptedException {
			String info = "";
			String value = input.toString();
			
			try {
				String splitter = "\t";
				
				if(value.contains("\t")) {
					// Ok
				} else {
					splitter = ",";
				}
				
				info += "splitter: " + splitter + ", ";
				
				String[] text_count_pair = value.split(splitter); //No idea what value we get here ...
				
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
				word.set(itr.nextToken().toLowerCase());
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
		//job.setReducerClass(TotalCountReducer.class);
		
		// We should use a decreasing order, based on the count key
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
		job.setCombinerClass(CountReducer.class); //This is the only line that changed between "Application 1 and 2"
		
		//TODO: Add combiner - each mapper has its own combiner, which is called after the mapper function, but before the reducer
		//TODO: Use Reducer as the combiner here -> pre-calculated occurances of specific word by summing the count, just like the Reducer does.
		
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
	
	private static int findLimit(Configuration conf, String hdfs_directory) throws IllegalArgumentException, IOException {
		FileStatus[] status = new FileStatus[0];
		Path p;
		try {
			p = new Path(hdfs_directory);
			status = p.getFileSystem(conf).listStatus(p);
		} catch(IllegalArgumentException e) {
			p = new Path(hdfs_directory.replaceFirst("s3", "s3a"));
			status = p.getFileSystem(conf).listStatus(p);
		}
		
		List<Integer> top_100 = new ArrayList<>();
		
		for(FileStatus s : status) {
			Path file_path = s.getPath();
			FSDataInputStream fsDataInputStream = p.getFileSystem(conf).open(file_path);
			BufferedReader br = null;
			
			try {
				br = new BufferedReader(new InputStreamReader(fsDataInputStream));
			    
				String l = null;
			    while((l = br.readLine())!= null){
			    	String[] values = l.split("\t");
			    	String word_count = values[0];
			    	int count = Integer.parseInt(word_count); // We found our answer
			    	if(top_100.size() != 100) {
			    		top_100.add(count); // Up until this point let's just add what we find directly to the top 100
			    		if(top_100.size() == 100) {
			    			Collections.sort(top_100); // Finally sort the current top 100 list
			    		}
			    	} else {
			    		// The current smallest value in top 100
			    		int smallest = top_100.get(0);
			    		if(count > smallest) {
			    			// We found a value that is larger than the smallest value in the top - it should be added and the previous smallest value removed
			    			top_100.remove(0);
			    			top_100.add(count);
			    			Collections.sort(top_100); //Sort the list afterwards
			    		} else {
			    			// In this data set (which we know is pre-sorted), there cannot be any more entries that are larger than anything in the list
			    			break;
			    		}
			    	}
			    	
			    }
			} finally {
				fsDataInputStream.close();
				if(br != null) {
					br.close();
				}
			}
		}

		if(top_100.size() == 0) {
			return 0;
		}
		
		return top_100.get(0); //Get the smallest value that fit into the top 100 highest values
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
			
			boolean second_job_ok = true;
			if(run_second_job) {
				// Let's then run another job that sorts the values in descending order by their values
				Job sort_by_value_job = createSortByValueJob(conf, intermediary_output_1, intermediary_output_2);
				
				second_job_ok = sort_by_value_job.waitForCompletion(true);
			}
			
			if(second_job_ok == false) {
				System.exit(1);
			}
			
			int limit = findLimit(conf, intermediary_output_2);
			conf.set("limit", "" + limit);
			
			Job filter_top_100 = createFilterTop100(conf, intermediary_output_2, final_output);
			
			boolean filter_job_ok = filter_top_100.waitForCompletion(true);
			
			System.exit(filter_job_ok ? 0 : 1);
		} else {
			System.exit(1);
		}
	}
}
