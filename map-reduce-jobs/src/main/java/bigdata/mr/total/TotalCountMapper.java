package cw.mr.total;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cw.mr.Constants;

public class TotalCountMapper extends Mapper<Object, Text, Text, DoubleWritable> {

	public static final String COST_COUNTER_GROUP = "cost";
	Text total = new Text("total");
	public static final Log log = LogFactory.getLog(TotalCountMapper.class);

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		try {
			/* Some condition satisfying it is header 
			 * https://stackoverflow.com/a/37543257
			 * */
			if (((LongWritable) key).get() == 0 && value.toString().contains("VendorID"))
				// filter header if exists.
				return;
			else {
				// For rest of data it goes here

				String valueString = value.toString();
				String[] tripData = valueString.split(","); // 18 columns
				// final column is the total amount for the trip
				// System.out.println(singleData);
				//log.info("xxxxxxXXXXXXXXXXXXXXXXXXXXXXXXXXXxxxxxxxxxxxxxxxxxxxxx");
				//log.info(tripData[0]);
				// for (int i = 0; i < tripData.length; i++) {
				// log.info(i + " == "+ tripData[i]);
				// }

				// ad all the values in this mapper to the counter increment
				// context.getCounter(COST_COUNTER_GROUP,
				// "total").increment(Long.valueOf(tripData[0]));
				context.write(total, new DoubleWritable(Double.valueOf(tripData[Constants.TOTAL_AMOUNT])));

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
