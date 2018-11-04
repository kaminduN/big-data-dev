package cw.mr.average;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Reducer;


public class AvgNumberOfPassengerReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>{

	public static final Log log = LogFactory.getLog(AvgNumberOfPassengerReducer.class);
	
	// in the Reducer class...
//	private Counters mapperCounter;
//
//	@Override
//	protected void setup(Context context) throws IOException, InterruptedException {
//
//		Configuration conf = context.getConfiguration();
//		JobClient client = new JobClient(conf);
//	    RunningJob parentJob = 
//	        client.getJob(JobID.forName( conf.get("mapred.job.id") ));
//	    mapperCounter = parentJob.getCounters();
////	    findCounter(AvgNumberOfPassengerMapper.AVERAGE_CALC_GROUP, name);
//	    
//	    Configuration conf = context.getConfiguration();
//	    JobClient client = new JobClient(conf);
////        Cluster cluster = new Cluster(conf);
//        Job currentJob = cluster.getJob(context.getJobID());
//        
//        mapperCounter = currentJob.getCounters().findCounter(COUNTER_NAME).getValue(); 
//	}

	
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		
		int sum = 0;
		double count = 0;
		log.info("----------- AvgNumberOfPassengerReducer - reduce -----------");
		log.info(key);

		for (IntWritable val : values) {
			log.info(val);
			sum += val.get();
			count += 1;
		}

		log.info(key.toString() + " : " + sum + " <-> " + count);
		log.info(sum/count);
//		log.info(context.getCounter(AvgNumberOfPassengerMapper.AVERAGE_CALC_GROUP, key.toString()).getValue());
//		log.info(mapperCounter.findCounter(AvgNumberOfPassengerMapper.AVERAGE_CALC_GROUP, key.toString()).getValue());

		context.write(key, new DoubleWritable( sum / count));
	}
	
	
}
