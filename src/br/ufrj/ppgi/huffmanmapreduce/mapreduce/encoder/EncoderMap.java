package br.ufrj.ppgi.huffmanmapreduce.mapreduce.encoder;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import br.ufrj.ppgi.huffmanmapreduce.Codification;
import br.ufrj.ppgi.huffmanmapreduce.Defines;
import br.ufrj.ppgi.huffmanmapreduce.SerializationUtility;
import br.ufrj.ppgi.huffmanmapreduce.mapreduce.io.BytesWritableEncoder;

public class EncoderMap extends
		Mapper<LongWritable, BytesWritable, LongWritable, BytesWritableEncoder> {

	LongWritable key = new LongWritable(0);
	Codification[] codificationArray = new Codification[Defines.twoPowerBitsCodification];
	BytesWritableEncoder buffer = new BytesWritableEncoder(Defines.writeBufferSize*1000);

	@Override
	protected void setup(
			Mapper<LongWritable, BytesWritable, LongWritable, BytesWritableEncoder>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		
		fileToCodification(context.getConfiguration());
	}

	public void map(LongWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		int valueLengthInBytes = value.getLength();
		for (int i = 0 ; i < valueLengthInBytes ; i++) {
			for (short j = 0; j < this.codificationArray.length; j++) {
				if (value.getBytes()[i] == codificationArray[j].symbol) {
					if(buffer.addCode(codificationArray[j]) == false) {
						context.write(this.key, buffer);
						buffer.clean();
						buffer.addCode(codificationArray[j]);
					}
					break;
				}
			}
		}
	}

	@Override
	protected void cleanup(
			Mapper<LongWritable, BytesWritable, LongWritable, BytesWritableEncoder>.Context context)
			throws IOException, InterruptedException {
		
		// Add EOF		
		for (short i = 0; i < this.codificationArray.length; i++) {
			if (codificationArray[i].symbol == 0) {
				if(buffer.addCode(codificationArray[i]) == false) {
					context.write(this.key, buffer);
					buffer.clean();
					buffer.addCode(codificationArray[i]);
				}
				break;
			}
		}
		
		if(buffer.length != 0)	{
			context.write(this.key, buffer);
		}
		
		super.cleanup(context);	
	}
	
	
	public void fileToCodification(Configuration configuration) throws IOException {
		FileSystem fileSystem = FileSystem.get(configuration);
		FSDataInputStream inputStream = fileSystem.open(new Path(configuration.get("fileName") + Defines.pathSuffix + Defines.codificationFileName));

		byte[] byteArray = new byte[inputStream.available()];
		inputStream.readFully(byteArray);
		
		this.codificationArray = SerializationUtility.deserializeCodificationArray(byteArray);
		
		
		/*
		System.out.println("CODIFICATION: symbol (size) code"); 
		for(short i = 0 ; i < symbols ; i++)
			System.out.println(codificationArray[i].toString());
		*/
	}
	
}