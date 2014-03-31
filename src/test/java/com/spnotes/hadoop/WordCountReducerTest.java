package com.spnotes.hadoop;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountReducerTest {

	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	
	@Before
	public void setup(){
		WordCountReducer wordCountReducer = new WordCountReducer();
		reduceDriver = ReduceDriver.newReduceDriver(new WordCountReducer());
	}
	
	@Test
	public void testSimpleReduce(){
		List<IntWritable> wordCountList = new ArrayList<IntWritable>();
		wordCountList.add(new IntWritable(1));
		wordCountList.add(new IntWritable(1));
		wordCountList.add(new IntWritable(1));
		
		reduceDriver.withInput(new Text("test"), wordCountList);
		reduceDriver.withOutput(new Text("test"), new IntWritable(3));
	}

	
}
