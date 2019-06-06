package com.hadoop.recommendation;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class UserFriendDataReducer extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {

	@Override
	public void reduce (TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	int sum =0,prod =1;
		for(IntWritable value :values) {
			sum =sum + value.get();
			prod = prod*value.get();
		}
		if(prod != 0 ) {
			context.write(key, new IntWritable(sum));

		}
	}
	

}
