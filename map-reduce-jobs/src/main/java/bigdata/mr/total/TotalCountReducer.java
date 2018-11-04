package cw.mr.total;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalCountReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	public static final Log log = LogFactory.getLog(TotalCountReducer.class);
	
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
						throws IOException, InterruptedException {
		log.info("---------TotalCountReducer - reduce ------------ ");
		double sum = 0;
		for (DoubleWritable doubleWritable : values) {
			sum += doubleWritable.get();
		}
		
		context.write(key, new DoubleWritable(sum));
	}
}
