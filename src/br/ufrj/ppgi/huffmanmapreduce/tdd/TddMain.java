package br.ufrj.ppgi.huffmanmapreduce.tdd;


public class TddMain {

	public static void main(String[] args) throws Exception {
		if(BytesWritableEncoderTests.test()) {
			System.out.println("Testes do BytesWritableEncoder ok!!");
		}
	}
}
