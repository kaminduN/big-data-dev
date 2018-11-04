package cw.mr.total;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class TotalCount {
	
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
        
        Job job = Job.getInstance(conf, "Count Total cost");
        job.setJarByClass(TotalCount.class);// class that contains the main method

        job.setMapperClass(TotalCountMapper.class);
//        job.setNumReduceTasks(0);  // no reducers calculation is done in the mapper it self..
        job.setReducerClass(TotalCountReducer.class);
        job.setCombinerClass(TotalCountReducer.class);
        
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);

        int code = job.waitForCompletion(true) ? 0 : 1;
        
//        if (code == 0) {
//        	Counter counter = job.getCounters().getGroup(TotalCountMapper.COST_COUNTER_GROUP).findCounter("total");
//        	System.out.println(counter.getDisplayName() + "\t" + counter.getValue());
//        }
//        else {
//        	System.out.println("error....");
//        }

        // Clean up empty output directory
//         FileSystem.get(conf).delete(outputDir, true);
        System.exit(code);
	}
}
