package com.hadoop.recommendation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinalReducer extends Reducer<Text, TextIntPair, Text, Text> {
	
		
		private List<TextIntPair> common = new ArrayList<TextIntPair>();
		@Override
		public void reduce(Text key, Iterable<TextIntPair> values, Context context) throws IOException, InterruptedException {
			common.clear();
			for(TextIntPair value :values) {
			
					Text first = new Text(value.getFirst());
					IntWritable second = new IntWritable(value.getSecond().get());
					if (common.size() < 10) {
						common.add(new TextIntPair(first,second));
						continue;
			}
					for(int i=0;i<10;i++) {
						if(common.get(i).getSecond().get()<second.get()) {
							common.set(i, new TextIntPair(first,second));
						}
					}
		}
			StringBuilder sb = new StringBuilder();
			for(int i=0;i<common.size();i++) {
				sb.append(common.get(i).getFirst().toString());sb.append("    ");
				sb.append(common.get(i).getSecond().toString());
				if(i<common.size()-1) {
					sb.append("    ");
				}
				}
			context.write(key, new Text(sb.toString()));
				}
	

}
