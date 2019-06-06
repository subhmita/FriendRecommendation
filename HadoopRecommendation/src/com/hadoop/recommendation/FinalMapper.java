package com.hadoop.recommendation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinalMapper extends Mapper<Object,Text,Text,TextIntPair>{
	
@Override
public void map(Object Key, Text value, Context context) throws IOException, InterruptedException {

//	A	B
	String line = value.toString();
	List<String> friendsList = new ArrayList<String>();
	String[] lineparts = line.split("\\t");	
	if(lineparts.length>1) {
	String sUserId = lineparts[0];
	String sFriends = lineparts[1];
	Integer noOfCommonFriend =Integer.parseInt(lineparts[2]);	
	context.write(new Text(sUserId), new TextIntPair(sFriends,noOfCommonFriend));

	}
	}
}
	
