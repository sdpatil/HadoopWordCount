package com.spnotes.hadoop;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class WordCountMapperTest {
	
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	
	@Before
	public void setup(){
		WordCountMapper mapper = new WordCountMapper();
		mapDriver = MapDriver.newMapDriver(mapper);//  <LongWritable, Text, Text, IntWritable>. 
	}

	@Test
	public void testOneWord() throws Exception{
		mapDriver.withInput(new LongWritable(1), new Text("This"));
		mapDriver.withOutput(new Text("This"), new IntWritable(1));
		mapDriver.runTest();
	}

	@Test
	public void testThreeWords() throws Exception{
		mapDriver.withInput(new LongWritable(1), new Text("This is test"));
		List<Pair<Text, IntWritable>> output = new ArrayList<Pair<Text,IntWritable>>();
		output.add(new Pair<Text, IntWritable>(new Text("This"), new IntWritable(1)));
		output.add(new Pair<Text, IntWritable>(new Text("is"), new IntWritable(1)));
		output.add(new Pair<Text, IntWritable>(new Text("test"), new IntWritable(1)));
		
		mapDriver.withAllOutput(output);
		mapDriver.runTest();
	}
	
	
	@Test
	public void testRepeatedWord() throws Exception{
		mapDriver.withInput(new LongWritable(1), new Text("test test test"));
		List<Pair<Text, IntWritable>> output = new ArrayList<Pair<Text,IntWritable>>();
		output.add(new Pair<Text, IntWritable>(new Text("test"), new IntWritable(1)));
		output.add(new Pair<Text, IntWritable>(new Text("test"), new IntWritable(1)));
		output.add(new Pair<Text, IntWritable>(new Text("test"), new IntWritable(1)));
		
		mapDriver.withAllOutput(output);
		mapDriver.runTest();
	}
}
