#!/bin/bash

mkdir -p bin
mkdir -p release

javac src/br/ufrj/ppgi/huffmanmapreduce/*.java src/br/ufrj/ppgi/huffmanmapreduce/mapreduce/encoder/*.java src/br/ufrj/ppgi/huffmanmapreduce/mapreduce/io/*.java src/br/ufrj/ppgi/huffmanmapreduce/mapreduce/symbolcount/*.java src/br/ufrj/ppgi/huffmanmapreduce/mapreduce/decoder/*.java -d bin

ant
