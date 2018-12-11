/**
 * QCRI, sPCA LICENSE
 * sPCA is a scalable implementation of Principal Component Analysis (PCA) on of Spark and MapReduce
 *
 * Copyright (c) 2015, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
*/
package org.qcri.sparkpca;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.log4j.Level;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.DoubleDoubleFunction;
import org.apache.mahout.math.function.Functions;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.QRDecomposition;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.storage.StorageLevel;
import org.qcri.sparkpca.FileFormat.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
/**
 * This code provides an implementation of PPCA: Probabilistic Principal
 * Component Analysis based on the paper from Tipping and Bishop:
 * 
 * sPCA: PPCA on top of Spark
 * 
 * 
 * @author Tarek Elgamal
 * 
 */

public class TallnWide implements Serializable {
	private final static Logger log = LoggerFactory.getLogger(SparkPCA.class);// getLogger(SparkPCA.class);
	private static final Logger logger = LoggerFactory.getLogger(SparkPCA.class);
	public static void main(String[] args) throws IOException {
		
		org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR);
		org.apache.log4j.Logger.getLogger("akka").setLevel(Level.ERROR);

		// Parsing input arguments
		final String inputPath;
		final String outputPath;
		final int nRows;
		final int nCols;
		final int nPCs;
		try {
			inputPath = System.getProperty("i");
			if (inputPath == null)
				throw new IllegalArgumentException();
		} catch (Exception e) {
			printLogMessage("i");
			return;
		}
		try {
			outputPath = System.getProperty("o");
			if (outputPath == null)
				throw new IllegalArgumentException();
		} catch (Exception e) {
			printLogMessage("o");
			return;
		}

		try {
			nRows = Integer.parseInt(System.getProperty("rows"));
		} catch (Exception e) {
			printLogMessage("rows");
			return;
		}

		try {
			nCols = Integer.parseInt(System.getProperty("cols"));
		} catch (Exception e) {
			printLogMessage("cols");
			return;
		}

		try {

			if (Integer.parseInt(System.getProperty("pcs")) == nCols) {
				nPCs = nCols - 1;
				System.out
						.println("Number of princpal components cannot be equal to number of dimension, reducing by 1");
			} else
				nPCs = Integer.parseInt(System.getProperty("pcs"));
		} catch (Exception e) {
			printLogMessage("pcs");
			return;
		}

		// Setting Spark configuration parameters
		SparkConf conf = new SparkConf().setAppName("TallnWide");//.setMaster("local[*]");// TODO
																						// remove
																						// this
																						// part
																						// for
																						// building
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryoserializer.buffer.max", "128m");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// log.info("Principal components computed successfully ");

		computePrincipalComponents(sc, inputPath, outputPath, nRows, nCols, nPCs);

		double allocatedMemory = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
		double presumableFreeMemory = Runtime.getRuntime().maxMemory() - allocatedMemory;
		System.out.println(presumableFreeMemory / Math.pow(1024, 3));

	}

	public static org.apache.spark.mllib.linalg.Matrix computePrincipalComponents(JavaSparkContext sc, String inputPath,
			String outputPath, final int nRows, final int nCols, final int nPCs) throws IOException {
		
				
		
		Matrix r = new DenseMatrix(nCols, nPCs);

		for (int i = 0; i < nCols; i++) {
			for (int j = i + 1; j < nPCs; j++) {
				r.set(i, j, 1);
			}
		}
		
		
		
//	      if (args.length<1) {
//	         logger.severe("1 arg is required :\n\t- hdfsmasteruri (8020 port) ex: hdfs://namenodeserver:8020");
//	         System.err.println("1 arg is required :\n\t- hdfsmasteruri (8020 port) ex: hdfs://namenodeserver:8020");
//	         System.exit(128);
//	      }
	      String hdfsuri = "hdfs://ec2-54-218-104-83.us-west-2.compute.amazonaws.com:9000";

	      String path="/user/hdfs/W/";
	      String fileName="W11";

	      // ====== Init HDFS File System Object
	      Configuration conf = new Configuration();
	      // Set FileSystem URI
	      conf.set("fs.defaultFS", hdfsuri);
	      // Because of Maven
	      conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	      conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
	      // Set HADOOP user
	      System.setProperty("HADOOP_USER_NAME", "hdfs");
	      System.setProperty("hadoop.home.dir", "/");
	      //Get the filesystem - HDFS
	      FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);

	      //==== Create folder if not exists
	      Path newFolderPath= new Path(path);
	      if(!fs.exists(newFolderPath)) {
	         // Create new Directory
	         fs.mkdirs(newFolderPath);
	         logger.error("Path "+path+" created.");
	      }

	      //==== Write file
	      logger.error("Begin Write file into hdfs");
	      //Create a path
	      Path hdfswritepath = new Path(newFolderPath + "/" + fileName);
	      //Init output stream
	      //Cassical output stream usage
	      SequenceFile.Writer writer=SequenceFile.createWriter(fs, conf, hdfswritepath
	    		  , IntWritable.class, MatrixWritable.class, CompressionType.BLOCK);
	      final IntWritable key = new IntWritable();
	      final MatrixWritable value = new MatrixWritable();
	      key.set(1);
	      value.set(r);
	      writer.append(key, value);
	      key.set(2);
	      value.set(r);
	      writer.append(key, value);
	      writer.close();
	      logger.error("End Write file into hdfs");

	      //==== Read file
	      logger.error("Read file into hdfs");
	      //Create a path
	      //Init input stream
	     
			
		JavaPairRDD<IntWritable, MatrixWritable> seqVectors = sc.sequenceFile("/user/hdfs/W/", IntWritable.class,
				MatrixWritable.class);

	      logger.error(seqVectors.collect().size()+"");
	      fs.close();
		
		return null;
	}

	private static void printLogMessage(String argName) {
		log.error("Missing arguments -D" + argName);
		log.info(
				"Usage: -Di=<path/to/input/matrix> -Do=<path/to/outputfolder> -Drows=<number of rows> -Dcols=<number of columns> -Dpcs=<number of principal components> [-DerrSampleRate=<Error sampling rate>] [-DmaxIter=<max iterations>] [-DoutFmt=<output format>] [-DComputeProjectedMatrix=<0/1 (compute projected matrix or not)>]");
	}
}
