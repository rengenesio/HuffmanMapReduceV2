package br.ufrj.ppgi.huffmanmapreduce.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;

import br.ufrj.ppgi.huffmanmapreduce.BitUtility;
import br.ufrj.ppgi.huffmanmapreduce.Codification;
import br.ufrj.ppgi.huffmanmapreduce.Defines;

public class BytesWritableEncoder extends BinaryComparable implements WritableComparable<BinaryComparable> {

	public int length, bits;
	public int index;
	public byte[] b;

	public BytesWritableEncoder() {
		this(Defines.writeBufferSize);
	}

	public BytesWritableEncoder(int capacity) {
		this.b = new byte[capacity];
		this.length = 0;
		this.bits = 0;
		this.index = 0;
	}
	
	@Override
	public byte[] getBytes() {
		return b;
	}

	@Override
	public int getLength() {
		return length;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.length = in.readInt();
		this.b = new byte[length];
		this.bits = in.readInt();
		in.readFully(b, 0, length);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(length);
		out.writeInt(bits);
		out.write(b, 0, length);
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}

	@Override
	public boolean equals(Object right_obj) {
		if (right_obj instanceof BytesWritableEncoder) {
			return super.equals(right_obj);
		}
		
		return false;
	}

	@Override
	public String toString() {
		String s = new String();
		for (int i = 0; i < length; i++) {
			s += Integer.toHexString((0xFF & b[i]) >> 4);
			s += Integer.toHexString(0xF & b[i]);
			s += " ";
		}

		s += "--> bits: " + this.bits + "(" + this.length + " bytes)";
		return s;
	}
	
	public boolean setCapacity(int new_cap) {
		byte[] new_data = null;
		try {
			new_data = new byte[new_cap];
		}
		catch (Error e) {
			System.out.println("Erro alocando BytesWritableEncoder de " + new_cap + " bytes. Máximo obtido: " + b.length + " bytes");
			return false;
		}
		
		System.arraycopy(this.b, 0, new_data, 0, this.length);
		this.b = new_data;
		
		return true;
	}

	private void addBit(boolean s) {
		BitUtility.setBit(this.b, this.bits, s);

		if (++this.bits % Defines.bitsInByte == 1) {
			this.length++;
		}
	}

	public boolean getBit(int pos) {
		return BitUtility.checkBit(this.b, pos);
	}

	public boolean addCode(Codification c) {
		if (this.b.length < this.length + (c.size / Defines.bitsInByte) + 1) {
			if(this.setCapacity(b.length * 3/2) == false) {
				return false;
			}
		}
				
		for (short i = 0; i < c.size; i++) {
			if (c.code[i] == 0) {
				this.addBit(false);
			}
			else {
				this.addBit(true);
			}
		}
		
		return true;
	}
	
	public boolean addSymbol(byte symbol) {
		if(this.index < b.length) {
			this.b[index] = symbol;
			
			this.index++;
			this.length++;
			
			return true;
		}

		return false;
	}
	
	public void clean() {
		int bitsMod = this.bits % Defines.bitsInByte;
		
		if(bitsMod != 0) {
			b[0] = b[this.bits / Defines.bitsInByte];
			this.length = 1;
			this.bits = bitsMod;
		}
		else {
			this.length = 0;
			this.bits = 0;
			this.index = 0;
		}
	}
}
