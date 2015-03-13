package br.ufrj.ppgi.huffmanmapreduce.mapreduce.decoder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import br.ufrj.ppgi.huffmanmapreduce.BitUtility;
import br.ufrj.ppgi.huffmanmapreduce.Codification;
import br.ufrj.ppgi.huffmanmapreduce.Defines;
import br.ufrj.ppgi.huffmanmapreduce.SerializationUtility;
import br.ufrj.ppgi.huffmanmapreduce.mapreduce.io.BytesWritableEncoder;

public class DecoderMap extends
		Mapper<LongWritable, BytesWritable, LongWritable, BytesWritableEncoder> {

	Codification[] codificationArray = new Codification[Defines.twoPowerBitsCodification];
	BytesWritableEncoder bufferOutput = new BytesWritableEncoder(Defines.writeBufferSize*1000);
	LongWritable key = new LongWritable(0);
//	
	boolean keySet = false;
	
	
	byte max_code = 0;
	HashMap<Integer, Byte> codificationMap = new HashMap<Integer, Byte>();
	
	int codificationArrayIndex = 0;
	
	@Override
	protected void setup(Mapper<LongWritable, BytesWritable, LongWritable, BytesWritableEncoder>.Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		fileToCodification(context.getConfiguration());
		codeToTreeArray();
	}
	

	public void map(LongWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		
		byte[] compressedByteArray = value.getBytes();
		int compressedBytesLengthInBits = value.getLength() * 8;
		
		for (int i = 0; i < compressedBytesLengthInBits ; i++) {
			codificationArrayIndex <<= 1;
			
			if (BitUtility.checkBit(compressedByteArray, i) == false) {
				codificationArrayIndex += 1;
			}
			else {
				codificationArrayIndex += 2;
			}
			
			if(codificationMap.containsKey(codificationArrayIndex)) {
				byte symbol = codificationMap.get(codificationArrayIndex);
//				System.out.println(String.format("i: %d   index: %d   -> %d", i, codificationArrayIndex, symbol));
				if(symbol != 0) {
					if(bufferOutput.addSymbol(symbol) == false) {
						context.write(this.key, bufferOutput);
						bufferOutput.clean();
						bufferOutput.addSymbol(symbol);
					}
				}
				else {
					return;
				}
				
				codificationArrayIndex = 0;
			}
		}
	}
	
	@Override
	protected void cleanup(
			Mapper<LongWritable, BytesWritable, LongWritable, BytesWritableEncoder>.Context context)
			throws IOException, InterruptedException {
		
		if(this.bufferOutput.length > 0) {
			context.write(this.key, bufferOutput);
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
		for(short i = 0 ; i < this.codificationArray.length ; i++)
			System.out.println(codificationArray[i].toString());
		*/
	}

	public void codeToTreeArray() {
		for(short i = 0 ; i < this.codificationArray.length ; i++) {
			this.max_code = (this.codificationArray[i].size > this.max_code) ? this.codificationArray[i].size : this.max_code;  
		}
		
		
		
	
		//codificationArrayElementSymbol = new byte[(int) Math.pow(2, (max_code + 1))];
		//codificationArrayElementUsed = new boolean[(int) Math.pow(2, (max_code + 1))];

		for (short i = 0; i < this.codificationArray.length; i++) {
			int index = 0;
			for (byte b : codificationArray[i].code) {
				index <<= 1;
				if (b == 0)
					index += 1;
				else
					index += 2;
			}
			
			codificationMap.put(index, codificationArray[i].symbol);
			
//			codificationArrayElementSymbol[index] = codificationArray[i].symbol;
//			codificationArrayElementUsed[index] = true;
		}
		
		///*
		System.out.println("codeToTreeArray():");
		System.out.println("TREE_ARRAY:"); 
//		for(int i = 0 ; i < Math.pow(2,(max_code + 1)) ; i++) 
//			if(codificationArrayElementUsed[i])
//				System.out.println("i: " + i + " -> " + codificationArrayElementSymbol[i]);
		for(Map.Entry<Integer, Byte> mapEntry : codificationMap.entrySet()) {
			System.out.println(mapEntry.getKey() + "  ->  " + mapEntry.getValue());			
		}
		System.out.println("------------------------------");
		//*/
	}
}