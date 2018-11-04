package cw.mr.count;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;


public class NumberOfRidesReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
//	MultipleOutputs<Text, IntWritable> mos;
	
//	@Override
//	protected void setup(Context context)
//			throws IOException, InterruptedException {
//		mos = new MultipleOutputs<Text, IntWritable>(context);
//	}
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

	
		int count = 0;
		for (IntWritable val : values) {
			count += val.get();
		}

//		
//		
//		if (key.getLength() == 2) { // hour
//			int sum = 0;
//			int count = 0;
//			
//			for (IntWritable val : values) {
//				sum += val.get();
//				count += 1;
//			}
//			
//			mos.write("passengerperhouroftheday", key, sum/count); // average 
//		}else if (key.getLength() > 4) { // year
//			
//			int count = 0;
//			for (IntWritable val : values) {
//				count += val.get();
//			}
//
//			mos.write("ridesparday", key, count);
//		} 
		context.write(key, new IntWritable(count));
	}
	
	
//	@Override
//	protected void cleanup(Context context)
//			throws IOException, InterruptedException {
//		mos.close();
//	}
}
