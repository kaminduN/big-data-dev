package cw.mr.average;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgNumberOfPassengerCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{

	public static final Log log = LogFactory.getLog(AvgNumberOfPassengerCombiner.class);
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	
		// sum all the keys to get the total sum. then pass those values to the reducer to be averaged
		
		int sum = 0;
		int count = 0;
		
		for (IntWritable val : values) {
			sum += val.get();
			count += 1;
		}
		log.info(key.toString() + " : " + sum + " <-> " + count);
		context.write(key, new IntWritable(sum));
	}
}
