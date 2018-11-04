package cw.mr.count;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cw.mr.Constants;

public class NumberOfRidesMapper extends Mapper<Object, Text, Text, IntWritable> {

	public static final Log log = LogFactory.getLog(NumberOfRidesMapper.class);
	private IntWritable one = new IntWritable(1);

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		try {
			/* Some condition satisfying it is header */
			if (((LongWritable) key).get() == 0 && value.toString().contains("VendorID"))
				// filter header if exists.
				return;
			else {
				// For rest of data it goes here

				String valueString = value.toString();
				String[] tripData = valueString.split(","); // 18 columns

				String date_time = tripData[Constants.PICKUP_DATETIME];
				//log.info(date_time);
				Text date = new Text(date_time.split(" ")[0]);

				// String time = date_time.split(" ")[1];
				// Text time_hour = new Text(time.split(":")[0]);
				// log.info(date + " -> " + time_hour.toString());

				context.write(date, one); // each date will get its own counter
				// context.write(time_hour, new
				// IntWritable(Integer.valueOf(tripData[Constants.PASSENGER_COUNT]))); // each
				// hour will get its passenger count

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
