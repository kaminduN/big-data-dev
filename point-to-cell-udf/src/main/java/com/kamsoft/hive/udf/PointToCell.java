package com.kamsoft.hive.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


@Description(name = "pointToCell", 
			 value = "returns 'cell', where cell is the cell belinging to the inputs", 
			 extended = "SELECT pointToCell(latitude, longitude) from foo limit 1;"
)
public class PointToCell extends UDF{
	// https://stackoverflow.com/questions/42040544/log-messages-in-hive-udf
	 private static final Log LOG = LogFactory.getLog(PointToCell.class.getName());
	/**
	 * To use 
	 * 	CREATE TEMPORARY FUNCTION pointtocell as 'com.kamsoft.hive.udf.PointToCell';
	 * @param latitude
	 * @param longitude
	 * @return cell
	 */
	public Text evaluate(Text latitude, Text longitude) {
		/*
		 * Entry point of the HIVE UDF
		 */
		if (latitude == null || longitude == null)
			return null;
		
		return new Text(String.valueOf(pointtoCellMapper(latitude.toString(), longitude.toString())));
	}

	public static int pointtoCellMapper(String latitude, String longitude) {
		
		/*
		 * (Q) For cell calculation, the latitude and longitude between points
			   40.845229, -74.019342 and 40.698187, -73.892880 will be derived in
			   10000 equal cells and the cell IDs will be assigned from 0 to 9999
			   (from left to right and top to bottom).
		
		 * Top left is 40.845229, -74.019342. top left cell is the (0,0) point and that is cell 0
		 * 
		 *                                          /\
		 * Using the latitude ==> (y) and longitude || (x) calculate the point.
		 * one we go the point (x0,y0) we can use y0*100+x0 to get the cell number.
		 * 
		 * 
		 * Calculated x0 and y0 are floored to get the cell point value. 
		 * 
		 * If latitude and longitude goes beyond the boundary they are considered illegal values and return -1
		 * 
		 * Values in the cell belonging to left and top boundaries are included into the cell it self
		 * while other boundaries belongs to the next cell. 
		 * 
		 * If x0,y0 becomes 100 (border values) they are put into the 99 points respectively ( special case).
		 * 
		 * coordinate representation 
		 * (-74.019342, 40.845229)
		 *             ---------------------
		 *             |                   |
		 *             y                   |  
		 *             |                   |
		 *             ------- x ----------- 
		 *                              (-73.892880, 40.698187)
		 * 
		 * Cell points (x0,y0)
		 * --------------------------------------------------
		 *  | 0,0  | 1,0  | 2,0  | . . .            | 99,0  |
		 *  | 0,1  | 1,1  | 2,1  | . . .            | 99,1  |
		 *  | 0,2  | 1,2  | 2,2  | . . .            | 99,2  |
		 *  .                                           .  
		 *  .                                           .
		 *  .                                           .
		 *  | 0,99 | 1,99 | 2,99 | . . .            | 99,99 |
		 *  -------------------------------------------------
		 * 
		 * Cell map
		 *  ------------------------------------------------
		 *  | 0    | 1    | 2    | . . .            |   99 |
		 *  | 100  | 101  | 102  | . . .            |  199 |
		 *  | 200  | 201  | 202  | . . .            |  299 |
		 *  .                                           .  
		 *  .                                           .
		 *  .                                           .
		 *  | 9900 | 9901 | 9902 | . . .            | 9999 |
		 *  ------------------------------------------------
		 *  
		 *  cell = x + 100 * y;
		 */
		
		int NUMBEROFCELLS = 100; // number of cells in one side

		double topLeftLongitude_x = -74.019342;
		double topLeftLatitude_y = 40.845229;
		
		double bottomRightLongitude_x = -73.892880;
		double bottomRightLatitude_y = 40.698187;
		
		double diffY = bottomRightLatitude_y - topLeftLatitude_y;
		double diffX = bottomRightLongitude_x - topLeftLongitude_x;
		
		double d_longitude = Double.valueOf(longitude);
		double d_latitude = Double.valueOf(latitude);

		// latitude increases when goes to top
		//  longitude decreases when goes to top, because of minus values
		if (d_longitude < topLeftLongitude_x || 
			d_longitude > bottomRightLongitude_x || 
			d_latitude > topLeftLatitude_y || 
			d_latitude < bottomRightLatitude_y) {
			LOG.fatal("invalid point... "+ d_latitude + " - " + d_longitude);
			return -1; // invalid values
		}
		// ((d_longitude - (-74.019342))/(-73.892880 - (-74.019342))) * 100 as x
		//  ((d_latitude - 40.845229)/(40.698187 - 40.845229) )* 100 as y
		
		double x = ((d_longitude - topLeftLongitude_x)/diffX) * NUMBEROFCELLS;
		LOG.debug("x: "+ x);
		double pointx = Math.floor(x);
		LOG.debug(pointx);
		
		double y = ((d_latitude - topLeftLatitude_y)/diffY )* NUMBEROFCELLS;  
		LOG.debug("y : "+ y);
		double pointy = Math.floor(y);
		LOG.debug(pointy);
		
		LOG.debug("x: "+ pointx + " y: "+ pointy);
		double cell = pointy * NUMBEROFCELLS + pointx;
		
		
		return (int)cell;
		
	}
		
		
}
