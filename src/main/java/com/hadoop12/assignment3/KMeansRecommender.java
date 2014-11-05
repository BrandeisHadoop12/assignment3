package com.hadoop12.assignment3;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;


public class KMeansRecommender {
	
    public static void main(String[] args) {
    	if (args.length != 3) {
    		System.err.println("Usage: <path_to_input> <path_to_output> <path_to_clusters>");
    		System.exit(2);
    	}
    	Configuration conf = new Configuration();
    	FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	// input path is the path to vectors
        Path inputPath = new Path(args[0]);
        // Output path is path to directory, which will contain resulting cluster data and the 
        // file containing the clusters that each review belongs to.
        String outputPath = args[1];
        Path clusterPath = new Path(args[2]);
        Path outputDir = new Path(outputPath);
        try {
			if (!fs.exists(clusterPath)) {
				if (!fs.mkdirs(clusterPath)) {
					System.err.println("Failed to construct clusters directory: " + clusterPath.toString());
					System.exit(2);
				}
			} else {
				System.err.println("Cluster directory already exists, exiting.");
				System.exit(2);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        try {
			if (!fs.exists(outputDir)) {
				if (!fs.mkdirs(outputDir)) {
					System.err.println("Failed to construct output directory: " + outputDir.toString());
					System.exit(2);
				}
			} else {
				System.err.println("Output directory already exists, exiting.");
				System.exit(2);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        String suffix = outputPath.endsWith("/") ? "product_clusters" : "/product_clusters";
        Path productClustersFile = new Path(outputPath + suffix);
        try {
			if (!fs.mkdirs(productClustersFile)) {
				System.err.println("Failed to construct product clusters directory: " + productClustersFile.toString());
				System.exit(2);
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        BufferedWriter productClustersWriter = null;
		try {
			Path productClustersOutputPath = new Path(productClustersFile.toString() + "/output");
//			if (!fs.createNewFile(productClustersOutputPath)) {
//				System.err.println("File not created: " + productClustersOutputPath.toString());
//				System.exit(2);
//			}
			productClustersWriter = new BufferedWriter(new OutputStreamWriter(fs.create(productClustersOutputPath)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Path initialClusters = null;
		try {
			initialClusters = RandomSeedGenerator.buildRandom(conf, 
					inputPath, 
					clusterPath, 
					10,
					new EuclideanDistanceMeasure());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        try {
			KMeansDriver.run(conf,
			        inputPath,
			        initialClusters,
			        new Path(outputPath),
			        0.001,
			        10,
			        true,
			        0,
			        true);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
 
        SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs,
			        new Path(outputPath + "/" + Cluster.CLUSTERED_POINTS_DIR + "/part-m-0"), conf);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
 
        IntWritable key = new IntWritable();
        WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
//        while (reader.next(key, value)) {
//            System.out.println(value.toString() + " belongs to cluster " + key.toString());
//        }
        
        try {
        	int i = 0;
			while (reader.next(key, value)) {
				i += 1;
				if (i < 20) {
					System.err.println(value.toString() + " belongs to cluster " + key.toString());
				}
				productClustersWriter.write(key.toString() + "\n");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        try {
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        try {
			productClustersWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
