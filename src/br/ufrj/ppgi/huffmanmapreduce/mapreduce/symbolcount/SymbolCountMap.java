package br.ufrj.ppgi.huffmanmapreduce.mapreduce.symbolcount;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import br.ufrj.ppgi.huffmanmapreduce.Defines;

public class SymbolCountMap extends Mapper<Object, BytesWritable, IntWritable, LongWritable>{
	long[] frequency = new long[Defines.twoPowerBitsCodification];
	
	int size;
	byte[] bytes;
	
	public void map(Object key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		size = value.getLength();
		bytes = value.getBytes();
		for (int i = 0 ; i < size ; i++) {
			frequency[bytes[i] & 0xFF]++;
		}
	}
	
	@Override
	protected void cleanup(
			Mapper<Object, BytesWritable, IntWritable, LongWritable>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
		
		for(int i = 0 ; i < Defines.twoPowerBitsCodification ; i++) {
			if(frequency[i] != 0) {
				context.write(new IntWritable(i), new LongWritable(frequency[i]));
			}
		}
	}	
}
