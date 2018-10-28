package com.kamsoft.hive.udf.point_to_cell;

import com.kamsoft.hive.udf.PointToCell;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for Hive UDF App.
 */
public class AppTest extends TestCase
{
    /**
     * The test case to test the UDF functionality
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    public void testUDFNullCheck() {
    	PointToCell example = new PointToCell();
    	Assert.assertNull(example.evaluate(null, null));
    }
    
    public void testUDFCellCheck() {
    	// "40.7863006592", "-73.9762115479" calculated to (x,y) 
    	// x: 34.000000 y: 40.000000 
    	// which translates to 4034.0
    	Assert.assertEquals(4034, PointToCell.pointtoCellMapper("40.7863006592", "-73.9762115479"));
    }
    
    public void testUDFInvalidPointCheck() {
    	// latitute out of range
    	int invalidInput = -1;
    	Assert.assertEquals(invalidInput, PointToCell.pointtoCellMapper("40.8585830688", "-73.9610061646"));
    	
    	// longitude out of range
    	Assert.assertEquals(invalidInput, PointToCell.pointtoCellMapper("40.7585830688", "-72.9610061646"));
    }
}
