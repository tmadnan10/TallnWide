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

public class AccumW implements Serializable {
	private final static Logger log = LoggerFactory.getLogger(SparkPCA.class);// getLogger(SparkPCA.class);
	private static final Logger logger = LoggerFactory.getLogger(SparkPCA.class);

	public static void main(String[] args) throws IOException, InterruptedException{
		org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR);
		org.apache.log4j.Logger.getLogger("akka").setLevel(Level.ERROR);

		// Parsing input arguments
		final String hdfsuri;
		final String path;
		final int nCols;
		final int nPCs;
		final int WIndex;
		final int round;
		// HDFS uri input
		try {
			hdfsuri = System.getProperty("hdfsuri");
			if (hdfsuri == null)
				throw new IllegalArgumentException();
		} catch (Exception e) {
			printLogMessage("i");
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
		
		try {
			WIndex = Integer.parseInt(System.getProperty("index"));
		} catch (Exception e) {
			printLogMessage("cols");
			return;
		}
		
		try {
			round = Integer.parseInt(System.getProperty("round"));
		} catch (Exception e) {
			printLogMessage("cols");
			return;
		}

		// Setting Spark configuration parameters
		SparkConf conf = new SparkConf().setAppName("AccumW");// .setMaster("local[*]");//
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

		computePrincipalComponents(sc, path, nCols, nPCs, round, WIndex, hdfsuri);

		double allocatedMemory = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
		double presumableFreeMemory = Runtime.getRuntime().maxMemory() - allocatedMemory;
		System.out.println(presumableFreeMemory / Math.pow(1024, 3));

	}
	
	public static void runCommand(String commandString) throws IOException, InterruptedException{
        System.out.println(commandString);
        Process p = Runtime.getRuntime().exec(commandString);
        p.waitFor();
    }

	public static org.apache.spark.mllib.linalg.Matrix computePrincipalComponents(JavaSparkContext sc,
			final String path, final int nCols, final int nPCs, final int round, final int index,
		 final String hdfsuri) throws IOException, InterruptedException {

		// path = /user/hdfs/
		// inputPath = /user/hdfs/W/WIndex/round+W+nodeID
		// checkPath = /user/hdfs/WCheck/WIndex/round+W+nodeID
		// outputPath = /user/hdfs/AccumXtX/WIndex/WIndex

		
		BufferedReader ID = new BufferedReader(new FileReader("ID"));
        String myID = ID.readLine();
        String partitionCountString = ID.readLine();
        int partitionCount = Integer.parseInt(partitionCountString);
        String dwString = ID.readLine();
        double dw = Double.parseDouble(dwString);
        String maxWnewString = ID.readLine();
        double maxWnew = Double.parseDouble(maxWnewString);
        ID.close();
		
		
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

		BufferedReader br = new BufferedReader(new FileReader("nodes"));
		String line = br.readLine();
		String[] nodes = line.split("\\s+");
		Boolean[] check = new Boolean[nodes.length];
		Path[] paths = new Path[nodes.length];

		for (int i = 0; i < nodes.length; i++) {
			System.out.println(hdfsuri + path + "/WCheck/W" + index + "/" + round + "W" + nodes[i]);
			paths[i] = new Path(hdfsuri + path + "/WCheck/W" + index + "/" + round + "W" + nodes[i]);
		  check[i] = false;
    }

		int count = nodes.length;

		while (count > 0) {
			for (int i = 0; i < nodes.length; i++) {
				if (!check[i] && fs.exists(paths[i])) {
					System.out.println(paths[i].getName()+" exists");
					count--;
					check[i] = true;
          fs.delete(paths[i]);
				}
			}
		}
    
    JavaPairRDD<IntWritable, MatrixWritable> seqVectors = null;


    Matrix prev = new DenseMatrix(nCols, nPCs);
    System.out.println(path + "AccumW/W" + index + "/");
    seqVectors = sc.sequenceFile(path + "AccumW/W" + index + "/", IntWritable.class, MatrixWritable.class);
    System.out.println("Previous W");
    prev = seqVectors.collect().get(0)._2.get();
    System.out.println(prev);    
		// ==== Read file
		logger.error("Read file into hdfs");
		// Create a path
		// Init input stream
		seqVectors = sc.sequenceFile(path+"/W/W"+index+"/", IntWritable.class,
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
    

		System.out.println("New W\n "+R);
		
		
		
		
		// basically our computation is finished, but we have to save it now
		// back to HDFS
		// as hdfs can locally save data, the codes below is a my version of
		// parallelizing

		// send same R to every node
		final Broadcast<Matrix> br_R = sc.broadcast(R);
		final String outputPath = path + "AccumW/W"+index+"/";
		// for each node save the file
		matrices.foreach(new VoidFunction<Matrix>() {

			public void call(Matrix yi) throws Exception {
				individuallySave(br_R.value(), outputPath, hdfsuri,index);
			}

		});

		// TODO check without the above parallelizing

		
		for (int p = 0; p < nCols; p++) {
            for (int q = 0; q < nPCs; q++) {
                maxWnew = Math.max(Math.abs(R.getQuick(p, q)), maxWnew);
            }
        }
        for (int p = 0; p < nCols; p++) {
            for (int q = 0; q < nPCs; q++) {
                dw = Math.max(Math.abs(prev.getQuick(p, q) - R.getQuick(p, q)), dw);
            }
        }
        double tolerance = 0.05;
        if(index == partitionCount ) { //last segment of W ..... endpoinnt of a round
            double sqrtEps = 2.2204e-16;
            dw /= (sqrtEps + maxWnew);
            if (dw <= tolerance) { //convergence achieved
                String commandString = "./writeDW.sh "+dw;
                runCommand(commandString);
                System.out.println("Convergence Achieved");
                commandString = "./conv.sh "+round;
                runCommand(commandString);
                commandString = "./write.sh "+myID;
                runCommand(commandString);
            }
            else { //no convergence
                String commandString = "./writeDW.sh "+dw;
                runCommand(commandString);
                System.out.println("dw of Round "+round+": "+dw);
                maxWnew = 0;
                dw = 0;
            }
        }
        
        
        
        String strToWrite = "";
        strToWrite = myID+"\\n"+partitionCount+"\\n"+dw+"\\n"+maxWnew;
        String commandString = "./write.sh "+strToWrite;
        runCommand(commandString);

        Path doneW = new Path(hdfsuri+path+round+"doneW"+index);
		    fs.createNewFile(doneW);
		    for (int i = 0; i < nodes.length; i++) {
      System.out.println(hdfsuri + path + "/W/W" + index + "/" + round + "W" + nodes[i]);
      Path p = new Path(hdfsuri + path + "/W/W" + index + "/" + round + "W" + nodes[i]);
      fs.delete(p);
    }
		return null;
	}

	public static void individuallySave(Matrix matrix, String path, String hdfsuri, int index) throws IOException {

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
		Path hdfswritepath = new Path(newFolderPath + "/" + "W" + index);// change the
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

