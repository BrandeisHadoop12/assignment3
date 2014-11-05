package com.hadoop12.assignment3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;


public class SeqWriter {

	public static void main(String args[]) throws IOException{
		
		if (args.length != 2) {
			System.err.println("Usage: <path_to_input> <path_to_output>");
			System.exit(2);
		}
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Scanner input = new Scanner(new BufferedReader(new InputStreamReader(fs.open(new Path(args[0])))));		
		Path outputPath = new Path(args[1]);

		SequenceFile.Writer writer = new SequenceFile.Writer(fs,  conf, outputPath, Text.class, Text.class);
		int i = 0;
		while (input.hasNextLine()) {
			i++;
			writer.append(new Text(String.valueOf(i)),new Text(input.nextLine()));
		}
		input.close();
		writer.close();

//		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path("appledata/apples"), conf);
//
//		Text key = new Text();
//		VectorWritable value = new VectorWritable();
//		while(reader.next(key, value)){
//			System.out.println(key.toString() + " , " + value.get().asFormatString());
//		}
//		reader.close();

	}

}

