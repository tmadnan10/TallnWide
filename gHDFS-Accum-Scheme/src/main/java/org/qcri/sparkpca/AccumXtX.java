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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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

import breeze.linalg.where;
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

public class AccumXtX implements Serializable {
	private final static Logger log = LoggerFactory.getLogger(SparkPCA.class);// getLogger(SparkPCA.class);
	private static final Logger logger = LoggerFactory.getLogger(SparkPCA.class);

	public static void main(String[] args) throws IOException {

		org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR);
		org.apache.log4j.Logger.getLogger("akka").setLevel(Level.ERROR);

		// Parsing input arguments
		final String hdfsuri;
		final String path;
		final String round;

		try {
			hdfsuri = System.getProperty("hdfsuri");
			if (hdfsuri == null)
				throw new IllegalArgumentException();
		} catch (Exception e) {
			printLogMessage("hdfsuri");
			return;
		}

		try {
			path = System.getProperty("i");
			if (path == null)
				throw new IllegalArgumentException();
		} catch (Exception e) {
			printLogMessage("i");
			return;
		}
		
		try {
			round = System.getProperty("round");
			if (round == null)
				throw new IllegalArgumentException();
		} catch (Exception e) {
			printLogMessage("round");
			return;
		}

		// Setting Spark configuration parameters
		SparkConf conf = new SparkConf().setAppName("AccumXtX");// .setMaster("local[*]");//
																// TODO
																// remove
																// this
																// part
																// for
																// building
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryoserializer.buffer.max", "128m");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// log.info("Principal components computed successfully ");

		computePrincipalComponents(sc, path, hdfsuri, round);

		double allocatedMemory = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
		double presumableFreeMemory = Runtime.getRuntime().maxMemory() - allocatedMemory;
		System.out.println(presumableFreeMemory / Math.pow(1024, 3));

	}

	public static org.apache.spark.mllib.linalg.Matrix computePrincipalComponents(JavaSparkContext sc, String path,
			final String hdfsuri, String round) throws IOException {

		// path = /user/hdfs/
		// inputPath = /user/hdfs
		// outputPath = /user/hdfs/AccumXtX/
		// path = /user/hdfs/
		// inputPath = /user/hdfs/XtX/round+XtX+nodeID
		// checkPath = /user/hdfs/XtXCheck/round+XtX+nodeID
		// outputPath = /user/hdfs/AccumXtX/XtX

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
		// Get the filesystem - HDFS
		FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);

		// check file in HDFS
		BufferedReader br = new BufferedReader(new FileReader("nodes"));
		String line = br.readLine();
		String[] nodes = line.split("\\s+");
		Boolean[] check = new Boolean[nodes.length];
		Path[] paths = new Path[nodes.length];

		for (int i = 0; i < nodes.length; i++) {
			System.out.println(hdfsuri + path + "/XtXCheck/" + round + "XtX" + nodes[i]);
			paths[i] = new Path(hdfsuri + path + "/XtXCheck/" + round + "XtX" + nodes[i]);
		  check[i] = false;
    }

		int count = nodes.length;
			
		while (count > 0) {
			for (int i = 0; i < nodes.length; i++) {
				System.out.println(paths[i].getName());
				if (!check[i] && fs.exists(paths[i])) {
					count--;
					check[i] = true;
					fs.delete(paths[i]);
				}
			}
		}

		// ==== Read file
		logger.error("Read file into hdfs");
		// Create a path
		// Init input stream
		// "/user/hdfs/W/"

		String inputPath = path + "/XtX/";
		JavaPairRDD<IntWritable, MatrixWritable> seqVectors = sc.sequenceFile(inputPath, IntWritable.class,
				MatrixWritable.class);
		JavaRDD<Matrix> matrices = seqVectors.map(new Function<Tuple2<IntWritable, MatrixWritable>, Matrix>() {

			public Matrix call(Tuple2<IntWritable, MatrixWritable> arg0) throws Exception {

				Matrix matrix = arg0._2.get();
				return matrix;
			}
		});

		Matrix R = matrices.treeReduce(new Function2<Matrix, Matrix, Matrix>() {

			@Override
			public Matrix call(Matrix v1, Matrix v2) throws Exception {
				// TODO Auto-generated method stub
				return v1.plus(v2);
			}
		});
		
		
		
		System.out.println(R);
		// basically our computation is finished, but we have to save it now
		// back to HDFS
		// as hdfs can locally save data, the codes below is a my version of
		// parallelizing

		// send same R to every node
		final Broadcast<Matrix> br_R = sc.broadcast(R);

		final String outputPath = path+"/AccumXtX/"; 
		// for each node save the file
		matrices.foreach(new VoidFunction<Matrix>() {

			public void call(Matrix yi) throws Exception {
				individuallySave(br_R.value(), outputPath, hdfsuri);
			}

		});

		// TODO check without the above parallelizing
		
		for (int i = 0; i < nodes.length; i++) {
			System.out.println(hdfsuri + path + "/XtX/" + round + "XtX" + nodes[i]);
			Path p = new Path(hdfsuri + path + "/XtX/" + round + "XtX" + nodes[i]);
			fs.delete(p);
		}
		
		Path XtXReady = new Path(hdfsuri+path+"XtXReady");
		fs.createNewFile(XtXReady);
		return null;
	}

	public static void individuallySave(Matrix matrix, String path, String hdfsuri) throws IOException {

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
		// Get the filesystem - HDFS
		FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);

		// ==== Create folder if not exists
		Path newFolderPath = new Path(path);
		if (!fs.exists(newFolderPath)) {
			// Create new Directory
			fs.mkdirs(newFolderPath);
			logger.error("Path " + path + " created.");
		}

		// ==== Write file
		logger.error("Begin Write file into hdfs");
		// Create a path
		Path hdfswritepath = new Path(newFolderPath + "/" + "XtX");// change the
																	// name as
																	// you see
																	// fit
																	// //TODO
		// Init output stream
		// Cassical output stream usage
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, hdfswritepath, IntWritable.class,
				MatrixWritable.class, CompressionType.BLOCK);
		final IntWritable key = new IntWritable();
		final MatrixWritable value = new MatrixWritable();
		key.set(0);
		value.set(matrix);
		writer.append(key, value);
		writer.close();
		logger.error("End Write file into hdfs");

	}

	private static void printLogMessage(String argName) {
		log.error("Missing arguments -D" + argName);
		log.info(
				"Usage: -Di=<path/to/input/matrix> -Do=<path/to/outputfolder> -Drows=<number of rows> -Dcols=<number of columns> -Dpcs=<number of principal components> [-DerrSampleRate=<Error sampling rate>] [-DmaxIter=<max iterations>] [-DoutFmt=<output format>] [-DComputeProjectedMatrix=<0/1 (compute projected matrix or not)>]");
	}
}

