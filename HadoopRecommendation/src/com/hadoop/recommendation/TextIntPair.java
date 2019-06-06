package com.hadoop.recommendation;
import java.io.DataInput; 
import java.io.DataOutput; 
import java.io.IOException; 
 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.Writable; 
 
public class TextIntPair implements Writable { 
	 
	private Text first; 
	private IntWritable second; 
	 
	public Text getFirst() { 
		return first; 
	} 
	public IntWritable getSecond() { 
		return second; 
	} 
	public TextIntPair(){ 
		first = new Text(); 
		second  = new IntWritable(); 
	} 
	public TextIntPair(String first, int second){ 
		this(new Text(first), new IntWritable(second)); 
	} 
	public TextIntPair(Text first, IntWritable second){ 
		this.first = first; 
		this.second = second; 
	} 
	public void readFields(DataInput in) throws IOException { 
		first.readFields(in); 
		second.readFields(in); 
	} 
	public void write(DataOutput out) throws IOException { 
		first.write(out); 
		second.write(out); 
		 
	} 
	public String toString() { 
		return first.toString() + "\t" + second.toString(); 
	} 
} 
	
