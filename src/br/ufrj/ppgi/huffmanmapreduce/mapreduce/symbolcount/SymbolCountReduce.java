package br.ufrj.ppgi.huffmanmapreduce.mapreduce.symbolcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import br.ufrj.ppgi.huffmanmapreduce.Defines;

public class SymbolCountReduce extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
	long[] frequency = new long[Defines.twoPowerBitsCodification];
	
	public void reduce(IntWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {

		long sum = 0;
		for (LongWritable val : values)
			sum += val.get();
		
		context.write(key, new LongWritable(sum));
	}
}