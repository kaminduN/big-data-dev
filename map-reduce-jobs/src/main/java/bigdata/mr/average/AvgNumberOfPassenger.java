package cw.mr.average;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class AvgNumberOfPassenger {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();// default
	    String[] otherArgs = new GenericOptionsParser(conf, args)
	            .getRemainingArgs();	
	    
	    
	    if (otherArgs.length != 2) {
	        System.err.println("Usage: CountNumUsersByState <input> <out>");
	        System.exit(2);
	    }
	
	    Path input = new Path(otherArgs[0]);
	    Path outputDir = new Path(otherArgs[1]);
	    
	    FileSystem.get(conf).delete(outputDir, true);
	    
	    Job job = Job.getInstance(conf, "Avg passengers per hour");
	    job.setJarByClass(AvgNumberOfPassenger.class);// class that contains the main method
	
	    job.setMapperClass(AvgNumberOfPassengerMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
	    // https://stackoverflow.com/a/17262734
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
    
	    
//	    job.setCombinerClass(AvgNumberOfPassengerCombiner.class);
//	    job.setCombinerClass(AvgNumberOfPassengerReducer.class);
        
	    job.setReducerClass(AvgNumberOfPassengerReducer.class);	    
	    job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
	    
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);

//        MultipleOutputs.addNamedOutput(job, "passengerperhouroftheday", TextOutputFormat.class, Text.class, IntWritable.class);
//        MultipleOutputs.addNamedOutput(job, "ridesparday", TextOutputFormat.class, Text.class, IntWritable.class);
        
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
	    
	}
}
