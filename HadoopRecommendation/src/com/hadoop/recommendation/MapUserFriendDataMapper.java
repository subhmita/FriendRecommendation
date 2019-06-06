package com.hadoop.recommendation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapUserFriendDataMapper extends Mapper<Object,Text,TextPair,IntWritable>{
	
@Override
public void map(Object Key, Text value, Context context) throws IOException, InterruptedException {

//	A	B
	String line = value.toString();
	List<String> friendsList = new ArrayList<String>();
	String[] lineparts = line.split("\\t");	
	if(lineparts.length>1) {
	String sUserId = lineparts[0];
	String sFriends = lineparts[1];
	StringTokenizer tokenizer =  new StringTokenizer(sFriends," ");
	while (tokenizer.hasMoreTokens()) {
		friendsList.add(tokenizer.nextToken());
	}
	for(int i=0;i< friendsList.size();i++) {
		for(int j=0;j<friendsList.size();j++) {
			if(j==i)
				continue;
			context.write(new TextPair(friendsList.get(i),friendsList.get(j)), new IntWritable(1));

		}
	}
	for(int i=0;i< friendsList.size();i++) {
		context.write(new TextPair(sUserId,friendsList.get(i)), new IntWritable(0));

	}
	}
}
	
}