package cw.mr;

public class Constants {
	// http://www.nyc.gov/html/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
	
	public static final int VendorID = 0; //1= Creative Mobile Technologies, LLC; 2= VeriFone Inc.
	public static final int PICKUP_DATETIME = 1;
	public static final int DROPOFF_DATETIME = 2;
	public static final int PASSENGER_COUNT = 3;
	public static final int Trip_distance = 4;
	public static final int Pickup_longitude = 5;
	public static final int Pickup_latitude = 6;
	
	public static final int RateCodeID = 7;
	/*
	 *  1= Standard rate
		2=JFK
		3=Newark
		4=Nassau or Westchester
		5=Negotiated fare
		6=Group ride
	 * */
	
	public static final int Store_and_fwd_flag = 8;
	/*
	 * Y= store and forward trip 
	   N= not a store and forward trip
	 */
	
	public static final int Dropoff_longitude = 9;
	public static final int Dropoff_latitude = 10;
	public static final int Payment_type = 11;
	/*
	 *  1= Credit card
		2= Cash
		3= No charge
		4= Dispute
		5= Unknown
		6= Voided trip
	 */
	
	public static final int Fare_amount = 12;
	public static final int Extra = 13;
	public static final int MTA_tax = 14;
	public static final int Improvement_surcharge = 15;
	public static final int Tip_amount = 16;
	public static final int Tolls_amount = 17;
	public static final int TOTAL_AMOUNT = 18;
}

