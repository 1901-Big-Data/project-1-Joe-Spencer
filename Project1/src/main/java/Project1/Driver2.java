package Project1;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Mapper.*;
import Reducer.*;

public class Driver2
{
    public static void main( String[] args ) throws Exception{	
    	if (args.length != 2) {
			System.out.printf(
					"Usage: WordCount <input dir> <output dir>\n");
			System.exit(-1);
		}
    	
    	Job job = new Job();
    	
    	job.setJarByClass(Driver2.class);
    	
    	job.setJobName("Job2");
    	
    	FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Q2Mapper.class);
		job.setReducerClass(Q2Reducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
    }
}
