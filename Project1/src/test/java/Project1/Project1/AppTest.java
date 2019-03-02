package Project1.Project1;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;


import Mapper.*;
import Reducer.*;

public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
	private MapDriver<LongWritable, Text, Text, FloatWritable> Q1mapDriver;

	
	private ReduceDriver<Text, FloatWritable, Text, FloatWritable> q1reduceDriver;
	
	private MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable> q1mapReduceDriver;
	
	private MapDriver<LongWritable, Text, Text, FloatWritable> Q2mapDriver;
	
	@Before
	public void setUp() {
		Q1Mapper q1mapper = new Q1Mapper();
		Q1mapDriver = new MapDriver<LongWritable, Text, Text, FloatWritable>();
		Q1mapDriver.setMapper(q1mapper);
		
		Q1Reducer q1reducer = new Q1Reducer();
		q1reduceDriver = new ReduceDriver<Text, FloatWritable, Text, FloatWritable>();
		q1reduceDriver.setReducer(q1reducer);
		
		q1mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable>();
		q1mapReduceDriver.setMapper(q1mapper);
		q1mapReduceDriver.setReducer(q1reducer);
	}
	
	public void testQ1map() {
		
		
		/*
		 * For this test, the mapper's input will be "1 cat cat dog" 
		 */
		Q1mapDriver.withInput(new LongWritable(1), new Text("\"Greece\",\"GRC\",\"Educational attainment, at least completed upper secondary, population 25+, female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"103.03693\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"98.2358\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"96.99316\",\"\",\"\",\"\",\"\",\"\",\"\","));
		
		/*
		 * The expected output is "cat 1", "cat 1", and "dog 1".
		 */
		Q1mapDriver.withOutput(new Text("Greece"), new FloatWritable((float) 103.03693));
		Q1mapDriver.withOutput(new Text("Greece"), new FloatWritable((float) 98.2358));
		Q1mapDriver.withOutput(new Text("Greece"), new FloatWritable((float) 96.99316));

		/*
		 * Run the test.
		 */
		Q1mapDriver.runTest();
	}
	public void testQ1Reducer() {
		
		

		List<FloatWritable> values = new ArrayList<FloatWritable>();
		values.add(new FloatWritable(25));
		values.add(new FloatWritable(35));
		values.add(new FloatWritable(15));
		/*
		 * For this test, the reducer's input will be "cat 1 1".
		 */
		q1reduceDriver.withInput(new Text("Latvia"), values);

		/*
		 * The expected output is "cat 2"
		 */
		q1reduceDriver.withOutput(new Text("Latvia"), new FloatWritable(25));

		/*
		 * Run the test.
		 */
		q1reduceDriver.runTest();
	}
	public void testq1MapReduce() {

		/*
		 * For this test, the mapper's input will be "1 cat cat dog" 
		 */
		q1mapReduceDriver.withInput(new LongWritable(1), new Text("\"Greece\",\"GRC\",\"Educational attainment, at least completed upper secondary, population 25+, female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"20\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"17.8\",\"\",\"0.05\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"7\",\"\",\"\",\"\",\"\",\"\",\"\","));


		/*
		 * The expected output (from the reducer) is "cat 2", "dog 1". 
		 */
		q1mapReduceDriver.addOutput(new Text("Greece"), new FloatWritable((float) 11.2125));

		/*
		 * Run the test.
		 */
		q1mapReduceDriver.runTest();
	}
	public void testQ2Mapper() {
		Q2Mapper q2mapper = new Q2Mapper();
		Q2mapDriver = new MapDriver<LongWritable, Text, Text, FloatWritable>();
		Q2mapDriver.setMapper(q2mapper);
		
		/*
		 * For this test, the mapper's input will be "1 cat cat dog" 
		 */
		Q2mapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Educational attainment, at least completed post-secondary, population 25+, female (%) (cumulative)\",\"SE.SEC.CUAT.PO.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"18.7\",\"\",\"\",\"\",\"\",\"22.2\",\"\",\"\",\"\",\"26.9\",\"28.10064\",\"28.02803\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"44.54951\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.37453\",\"36.00504\",\"37.52263\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","));

		/*
		 * The expected output is "cat 1", "cat 1", and "dog 1".
		 */
		Q2mapDriver.withOutput(new Text("United States"), new FloatWritable((float) 35.37453));
		Q2mapDriver.withOutput(new Text("United States"), new FloatWritable((float) 36.00504));
		Q2mapDriver.withOutput(new Text("United States"), new FloatWritable((float) 37.52263));
		
		

		/*
		 * Run the test.
		 */
		Q2mapDriver.runTest();
	}
	

}