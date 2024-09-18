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
	
	private MapDriver<LongWritable, Text, Text, FloatWritable> q2mapDriver;
	
	private ReduceDriver<Text, FloatWritable, Text, FloatWritable> q2reduceDriver;
	
	private MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable> q2mapReduceDriver;
	
	private MapDriver<LongWritable, Text, Text, FloatWritable> q3mapDriver;
	
	private ReduceDriver<Text, FloatWritable, Text, FloatWritable> q3reduceDriver;
	
	private MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable> q3mapReduceDriver;
	
	private MapDriver<LongWritable, Text, Text, FloatWritable> q4mapDriver;
	
	private ReduceDriver<Text, FloatWritable, Text, FloatWritable> q4reduceDriver;
	
	private MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable> q4mapReduceDriver;
	
	private MapDriver<LongWritable, Text, Text, FloatWritable> q5mapDriver;
	
	private ReduceDriver<Text, FloatWritable, Text, FloatWritable> q5reduceDriver;
	
	private MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable> q5mapReduceDriver;
	
	
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
		
		Q2Mapper q2mapper = new Q2Mapper();
		q2mapDriver = new MapDriver<LongWritable, Text, Text, FloatWritable>();
		q2mapDriver.setMapper(q2mapper);
		
		Q2Reducer q2reducer = new Q2Reducer();
		q2reduceDriver = new ReduceDriver<Text, FloatWritable, Text, FloatWritable>();
		q2reduceDriver.setReducer(q2reducer);
		
		q2mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable>();
		q2mapReduceDriver.setMapper(q2mapper);
		q2mapReduceDriver.setReducer(q2reducer);
		
		Q3Mapper q3mapper = new Q3Mapper();
		q3mapDriver = new MapDriver<LongWritable, Text, Text, FloatWritable>();
		q3mapDriver.setMapper(q3mapper);
		
		Q3Reducer q3reducer = new Q3Reducer();
		q3reduceDriver = new ReduceDriver<Text, FloatWritable, Text, FloatWritable>();
		q3reduceDriver.setReducer(q3reducer);
		
		q3mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable>();
		q3mapReduceDriver.setMapper(q3mapper);
		q3mapReduceDriver.setReducer(q3reducer);
		
		Q4Mapper q4mapper = new Q4Mapper();
		q4mapDriver = new MapDriver<LongWritable, Text, Text, FloatWritable>();
		q4mapDriver.setMapper(q4mapper);
		
		Q4Reducer q4reducer = new Q4Reducer();
		q4reduceDriver = new ReduceDriver<Text, FloatWritable, Text, FloatWritable>();
		q4reduceDriver.setReducer(q4reducer);
		
		q4mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable>();
		q4mapReduceDriver.setMapper(q4mapper);
		q4mapReduceDriver.setReducer(q4reducer);
		
		Q5Mapper q5mapper = new Q5Mapper();
		q5mapDriver = new MapDriver<LongWritable, Text, Text, FloatWritable>();
		q5mapDriver.setMapper(q5mapper);
		
		Q5Reducer q5reducer = new Q5Reducer();
		q5reduceDriver = new ReduceDriver<Text, FloatWritable, Text, FloatWritable>();
		q5reduceDriver.setReducer(q5reducer);
		
		q5mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable>();
		q5mapReduceDriver.setMapper(q5mapper);
		q5mapReduceDriver.setReducer(q5reducer);
	}
	
	public void testQ1map() {
		
		
		
		Q1mapDriver.withInput(new LongWritable(1), new Text("\"Greece\",\"GRC\",\"Educational attainment, at least completed upper secondary, population 25+, female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"103.03693\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"98.2358\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"96.99316\",\"\",\"\",\"\",\"\",\"\",\"\","));
		
		Q1mapDriver.withOutput(new Text("Greece"), new FloatWritable((float) 103.03693));
		Q1mapDriver.withOutput(new Text("Greece"), new FloatWritable((float) 98.2358));
		Q1mapDriver.withOutput(new Text("Greece"), new FloatWritable((float) 96.99316));

		
		Q1mapDriver.runTest();
	}
	public void testQ1Reducer() {
		
		

		List<FloatWritable> values = new ArrayList<FloatWritable>();
		values.add(new FloatWritable(25));
		values.add(new FloatWritable(35));
		values.add(new FloatWritable(15));
		
		
		q1reduceDriver.withInput(new Text("Latvia"), values);

		
		q1reduceDriver.withOutput(new Text("Latvia"), new FloatWritable(25));

		
		q1reduceDriver.runTest();
	}
	public void testq1MapReduce() {

		//input matches data set structure but is fictional
		q1mapReduceDriver.withInput(new LongWritable(1), new Text("\"Greece\",\"GRC\",\"Educational attainment, at least completed upper secondary, population 25+, female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"20\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"17.8\",\"\",\"0.05\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"7\",\"\",\"\",\"\",\"\",\"\",\"\","));


		//Expect output is Greece and 11.2125
		q1mapReduceDriver.addOutput(new Text("Greece"), new FloatWritable((float) 11.2125));

		//run test
		q1mapReduceDriver.runTest();
	}
	public void testQ2Mapper() {
		
		//input matches data set structure
		q2mapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Educational attainment, at least completed post-secondary, population 25+, female (%) (cumulative)\",\"SE.SEC.CUAT.PO.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"18.7\",\"\",\"\",\"\",\"\",\"22.2\",\"\",\"\",\"\",\"26.9\",\"28.10064\",\"28.02803\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"44.54951\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.37453\",\"36.00504\",\"37.52263\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","));

		//expected (United States, 35.37453) 
		q2mapDriver.withOutput(new Text("United States"), new FloatWritable((float) 35.37453));
		q2mapDriver.withOutput(new Text("United States"), new FloatWritable((float) 36.00504));
		q2mapDriver.withOutput(new Text("United States"), new FloatWritable((float) 37.52263));
		
		

		//run test
		q2mapDriver.runTest();
	}
	public void testQ2Reducer() {
		
		

		List<FloatWritable> values = new ArrayList<FloatWritable>();
		values.add(new FloatWritable((float) 35.37453));
		values.add(new FloatWritable((float) 36.00504));
		values.add(new FloatWritable((float) 37.52263));
		
		
		q2reduceDriver.withInput(new Text("United States"), values);

		
		q2reduceDriver.withOutput(new Text("United States"), new FloatWritable((float) 1.4320654));

		
		q2reduceDriver.runTest();
	}
	public void testq2MapReduce() {

		//input matches data set structure but is fictional
		q2mapReduceDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Educational attainment, at least completed post-secondary, population 25+, female (%) (cumulative)\",\"SE.SEC.CUAT.PO.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"18.7\",\"\",\"\",\"\",\"\",\"22.2\",\"\",\"\",\"\",\"26.9\",\"28.10064\",\"28.02803\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"44.54951\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.37453\",\"36.00504\",\"37.52263\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","));


		//
		q2mapReduceDriver.addOutput(new Text("United States"), new FloatWritable((float) 1.4320654));

		//run test
		q2mapReduceDriver.runTest();
	}
	public void testQ3Mapper() {
		
		//input matches data set structure
		q3mapDriver.withInput(new LongWritable(1), new Text("\"Belize\",\"BLZ\",\"Employment to population ratio, 15+, male (%) (national estimate)\",\"SL.EMP.TOTL.SP.FE.NE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"27.6599998474121\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.0999984741211\",\"\",\"41.7900009155273\",\"\",\"\",\"43.7900009155273\",\"\","));

		q3mapDriver.addOutput(new Text("Belize"), new FloatWritable((float) 35.1));
		q3mapDriver.addOutput(new Text("Belize"), new FloatWritable((float) 41.79));
		q3mapDriver.addOutput(new Text("Belize"), new FloatWritable((float) 43.79));
		
		//run test
		q3mapDriver.runTest();
	}
	public void testQ3Reducer() {
		
	List<FloatWritable> values = new ArrayList<FloatWritable>();
	values.add(new FloatWritable((float) 35.37453));
	values.add(new FloatWritable((float) 36.00504));
	values.add(new FloatWritable((float) 37.52263));
	
	
	q3reduceDriver.withInput(new Text("United States"), values);

	
	q3reduceDriver.withOutput(new Text("United States"), new FloatWritable((float) 2.148098));

	
	q3reduceDriver.runTest();
	}
	public void testq3MapReduce() {

		//input matches data set structure but is fictional
		q3mapReduceDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Employment to population ratio, 15+, male (%) (national estimate)\",\"SE.SEC.CUAT.PO.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"18.7\",\"\",\"\",\"\",\"\",\"22.2\",\"\",\"\",\"\",\"26.9\",\"28.10064\",\"28.02803\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"44.54951\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.37453\",\"36.00504\",\"37.52263\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","));


		//
		q3mapReduceDriver.addOutput(new Text("United States"), new FloatWritable((float) 2.148098));

		//run test
		q3mapReduceDriver.runTest();
	}
public void testQ4Mapper() {
		
		//input matches data set structure
		q4mapDriver.withInput(new LongWritable(1), new Text("\"Belize\",\"BLZ\",\"Employment to population ratio, 15+, female (%) (national estimate)\",\"SL.EMP.TOTL.SP.FE.NE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"27.6599998474121\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.0999984741211\",\"\",\"41.7900009155273\",\"\",\"\",\"43.7900009155273\",\"\","));

		q4mapDriver.addOutput(new Text("Belize"), new FloatWritable((float) 35.1));
		q4mapDriver.addOutput(new Text("Belize"), new FloatWritable((float) 41.79));
		q4mapDriver.addOutput(new Text("Belize"), new FloatWritable((float) 43.79));
		
		//run test
		q4mapDriver.runTest();
	}
	public void testQ4Reducer() {
		
	List<FloatWritable> values = new ArrayList<FloatWritable>();
	values.add(new FloatWritable((float) 35.37453));
	values.add(new FloatWritable((float) 36.00504));
	values.add(new FloatWritable((float) 37.52263));
	
	
	q4reduceDriver.withInput(new Text("United States"), values);

	
	q4reduceDriver.withOutput(new Text("United States"), new FloatWritable((float) 2.148098));

	
	q4reduceDriver.runTest();
	}
	public void testq4MapReduce() {

		//input matches data set structure but is fictional
		q4mapReduceDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Employment to population ratio, 15+, female (%) (national estimate)\",\"SE.SEC.CUAT.PO.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"18.7\",\"\",\"\",\"\",\"\",\"22.2\",\"\",\"\",\"\",\"26.9\",\"28.10064\",\"28.02803\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"44.54951\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.37453\",\"36.00504\",\"37.52263\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","));


		//
		q4mapReduceDriver.addOutput(new Text("United States"), new FloatWritable((float) 2.148098));

		//run test
		q4mapReduceDriver.runTest();
	}
	public void testQ5map() {
		
		
		
		q5mapDriver.withInput(new LongWritable(1), new Text("\"Bosnia and Herzegovina\",\"BIH\",\"Firms with female participation in ownership (% of firms)\",\"IC.FRM.FEMO.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"21.6\",\"\",\"\",\"\",\"32.8\",\"\",\"\",\"\",\"27.2\",\"\",\"\",\"\","));
		q5mapDriver.withOutput(new Text("Bosnia and Herzegovina"), new FloatWritable((float) 21.6));
		q5mapDriver.withOutput(new Text("Bosnia and Herzegovina"), new FloatWritable((float) 32.8));
		q5mapDriver.withOutput(new Text("Bosnia and Herzegovina"), new FloatWritable((float) 27.2));

		
		q5mapDriver.runTest();
	}
	public void testQ5Reducer() {
		
		

		List<FloatWritable> values = new ArrayList<FloatWritable>();
		values.add(new FloatWritable((float) 21.6));
		values.add(new FloatWritable((float) 32.8));
		values.add(new FloatWritable((float) 27.2));
		
		
		q5reduceDriver.withInput(new Text("Bosnia and Herzegovina"), values);

		
		q5reduceDriver.withOutput(new Text("Bosnia and Herzegovina"), new FloatWritable((float) 27.200003));

		
		q5reduceDriver.runTest();
	}
	public void testq5MapReduce() {

		//input matches data set structure but is fictional
		q5mapReduceDriver.withInput(new LongWritable(1), new Text("\"Bosnia and Herzegovina\",\"BIH\",\"Firms with female participation in ownership (% of firms)\",\"IC.FRM.FEMO.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"21.6\",\"\",\"\",\"\",\"32.8\",\"\",\"\",\"\",\"27.2\",\"\",\"\",\"\","));
		

		q5mapReduceDriver.addOutput(new Text("Bosnia and Herzegovina"), new FloatWritable((float) 27.200003));

		//run test
		q5mapReduceDriver.runTest();
	}
	
}