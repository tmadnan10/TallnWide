
package org.csebuet.tallnwide;
import java.io.InputStreamReader;
import java.io.File;
import java.io.PrintStream;
import java.io.Serializable;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner; 
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Level;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
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
import org.netlib.util.doubleW;
import org.netlib.util.intW;
import org.csebuet.tallnwide.FileFormat.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import com.jezhumble.javasysmon.JavaSysMon;
import scala.Tuple2;



public class TallnWide implements Serializable {
    private final static Logger log = LoggerFactory.getLogger(SparkPCA.class);// getLogger(SparkPCA.class);
    
    // variables for stats
    static String dataset = "amazon.txt1000000x1000.seq";
    static long startTime, endTime, totalTime;
    public static Stat stat = new Stat();
    
    public static void main(String[] args) throws InterruptedException, IOException{
        
        org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR);
        org.apache.log4j.Logger.getLogger("akka").setLevel(Level.ERROR);
        
        // Parsing input arguments
        // String dataSet;
        final String inputPath;
        final String outputPath;
        final int nRows;
        final int nCols;
        final double avgCols;
        final int nPCs;
        final double tolerance;
        final int maxIter;
        final int rho;
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
            dataset = System.getProperty("dataSet");
            if (dataset == null)
                throw new IllegalArgumentException();
        } catch (Exception e) {
            printLogMessage("dataSet");
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
            avgCols = Integer.parseInt(System.getProperty("avgCols"));
        } catch (Exception e) {
            printLogMessage("avgCols");
            return;
        }
        
        try {
            tolerance = Double.parseDouble(System.getProperty("tolerance"));
        } catch (Exception e) {
            printLogMessage("toleance");
            return;
        }
        
        try {
            maxIter = Integer.parseInt(System.getProperty("maxIter"));
        } catch (Exception e) {
            printLogMessage("maxIter");
            return;
        }
        
        try {
            rho = Integer.parseInt(System.getProperty("rho"));
        } catch (Exception e) {
            printLogMessage("rho");
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
        
        //*********************************************************************************************************************
        Process p1;
        p1 = Runtime.getRuntime().exec("./deleteDummy.sh");
        //*********************************************************************************************************************/
        
        // log.info("Principal components computed successfully ");
        
        computePrincipalComponents(sc, inputPath, outputPath, nRows, nCols, avgCols, nPCs, maxIter, tolerance,
                                   rho);
        
        
    }
    
    public static org.apache.spark.mllib.linalg.Matrix computePrincipalComponents(JavaSparkContext sc, String inputPath,
                                                                                  String outputPath, final int nRows, final int nCols, final double avgCols, final int nPCs,
                                                                                  final int maxIter, final double tolerance, final int maxMemory) throws InterruptedException, IOException {
        BufferedReader masterFile = new BufferedReader(new FileReader("masterFile"));
        String masterID = masterFile.readLine();
        
        BufferedReader ID = new BufferedReader(new FileReader("ID"));
        String myID = ID.readLine();
        System.out.println(masterID+" "+myID);
        boolean masterBool = false;
        
        if(Integer.parseInt(masterID) == Integer.parseInt(myID)) {
            masterBool = true;
        }
        
        BufferedReader nodeList = new BufferedReader(new FileReader("nodes"));
        int [] nodes = new int[1];
        File [] XtXFiles;
        String line = "";
        if(masterBool){
            //If I am the master
            //load node list
            line = nodeList.readLine();
            String [] splittednodes = line.split("\\s+");
            nodes = new int[splittednodes.length];
            XtXFiles = new File[splittednodes.length];
            for (int i = 0; i < splittednodes.length; i++) {
                nodes[i] = Integer.parseInt(splittednodes[i]);
                System.out.println(nodes[i]);
                XtXFiles[i] = new File("XtX"+splittednodes[i]);
            }
        }
        
        
        
        stat.appName = "TallnWide";
        stat.dataSet = dataset;
        stat.nRows = nRows;
        stat.nCols = nCols;
        stat.nPCs = nPCs;
        PCAUtils.printStatToFile(stat, outputPath);
        JavaPairRDD<IntWritable, VectorWritable> seqVectors = sc.sequenceFile(inputPath, IntWritable.class,
                                                                              VectorWritable.class);
        long s = seqVectors.count();
        
        JavaRDD<Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>> vectors = seqVectors.map(
                                                                                                new Function<Tuple2<IntWritable, VectorWritable>, Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>>() {
                                                                                                    
                                                                                                    public Tuple2<Integer, org.apache.spark.mllib.linalg.Vector> call(
                                                                                                                                                                      Tuple2<IntWritable, VectorWritable> arg0) throws Exception {
                                                                                                        Integer index = arg0._1.get();
                                                                                                        
                                                                                                        org.apache.mahout.math.Vector mahoutVector = arg0._2.get();
                                                                                                        Iterator<Element> elements = mahoutVector.nonZeroes().iterator();
                                                                                                        ArrayList<Tuple2<Integer, Double>> tupleList = new ArrayList<Tuple2<Integer, Double>>();
                                                                                                        while (elements.hasNext()) {
                                                                                                            Element e = elements.next();
                                                                                                            if (e.index() >= nCols || e.get() == 0)
                                                                                                                continue;
                                                                                                            Tuple2<Integer, Double> tuple = new Tuple2<Integer, Double>(e.index(), e.get());
                                                                                                            tupleList.add(tuple);
                                                                                                        }
                                                                                                        Tuple2<Integer, org.apache.spark.mllib.linalg.Vector> sparkVector = new Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>(
                                                                                                                                                                                                                                      index, Vectors.sparse(nCols, tupleList));
                                                                                                        
                                                                                                        return sparkVector;
                                                                                                    }
                                                                                                }).persist(StorageLevel.MEMORY_ONLY_SER()); // TODO
        // change
        // later;
        
        seqVectors = null;
        System.gc ();
        System.runFinalization ();
        
        // 1. Mean Job : This job calculates the mean and span of the columns of
        // the input RDD<org.apache.spark.mllib.linalg.Vector>
        final Accumulator<double[]> matrixAccumY = sc.accumulator(new double[nCols], new VectorAccumulatorParam());
        final double[] internalSumY = new double[nCols];
        vectors.foreachPartition(new VoidFunction<Iterator<Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>>>() {
            
            public void call(Iterator<Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>> arg0) throws Exception {
                org.apache.spark.mllib.linalg.Vector yi;
                int[] indices = null;
                int i;
                while (arg0.hasNext()) {
                    yi = arg0.next()._2;
                    indices = ((SparseVector) yi).indices();
                    for (i = 0; i < indices.length; i++) {
                        internalSumY[indices[i]] += yi.apply(indices[i]);
                    }
                }
                matrixAccumY.add(internalSumY);
            }
            
        });// End Mean Job
        
        
        // Get the sum of column Vector from the accumulator and divide each
        // element by the number of rows to get the mean
        // not best of practice to use non-final variable
        final Vector meanVector = new DenseVector(matrixAccumY.value()).divide(nRows);
        // matrixAccumY = null;
        final Broadcast<Vector> br_ym_mahout = sc.broadcast(meanVector);
        
        // TODO modify here for dynamically selecting appropriate partition for
        // W
        // dynamically set and assign range
        
        // hardcoded for now
        //JavaSysMon monitor =   new JavaSysMon();
        //String osName =        monitor.osName();
        
        //System.out.println("Amount of total memory free " + monitor.physical().getFreeBytes());
        //System.out.println("JVM Memory:"+monitor.physicalWithBuffersAndCached().getTotalBytes());
        final Accumulator<String> leastMem = sc.accumulator(new String(""),
                                                            new StringAccumulatorParam());
        
        
        final long allocatedMemory      = (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory());
        final long presumableFreeMemory = (Runtime.getRuntime().maxMemory() - allocatedMemory)/(1024*1024);
        
        System.out.println("Presumable Memory in Master"+presumableFreeMemory);
        
        vectors.foreachPartition(new VoidFunction<Iterator<Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>>>() {
            
            public void call(Iterator<Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>> arg0) throws Exception {
                final long allocatedMemory      = (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory());
                final double presumableFreeMemory = (Runtime.getRuntime().maxMemory() - allocatedMemory)/(1024*1024);
                leastMem.add(presumableFreeMemory+",");
                System.out.println("Presumable Memory in Slave (will not print)"+presumableFreeMemory);
                
            }
            
        });// End Mean Job
        
        System.out.println(leastMem.value());
        String[] splitted = leastMem.value().split(",");
        double min=Double.parseDouble(splitted[0]);
        for (int i=1; i < splitted.length; i++)
        {
            if (Double.parseDouble(splitted[i])<min)
                min=Double.parseDouble(splitted[i]);
        }
        
        System.out.println("Minimum Memory"+min);
        final double sizeOfW = (maxMemory * nCols * nPCs * 8) / 1024 / 1024;//4 na koto pore chinta kori
        System.out.println("Size of W"+sizeOfW);
        final int partitionCount = (int) Math.ceil(sizeOfW / min);
        
        System.out.println("No of Partition of W: " + partitionCount);
        //partitionCount + 1;
	int nC = partitionCount + 1;//+4;
        stat.nPartitions = nC - 1;
        
        int range[] = new int[nC];
        for (int i = 0; i < range.length; i++) {
            range[i] = i * nCols / (nC - 1);
        }
        
        Process p;
        
        if(masterBool){
            String strToWrite = (nC-1)+"\\n0\\n0";
            String commandString = "./append.sh "+strToWrite;
            System.out.println(commandString);
            p = Runtime.getRuntime().exec(commandString);
            p.waitFor();
        }
        
    
        
        
        
        System.out.println(range.length+"Generating Initial W's");
        
        long IOTimeStart, IOTimeEnd, totalIOTime = 0;
        
        if(masterBool){
            System.out.println("Master Node\nSending Initial W to All Nodes");
            for (int i = 1; i < range.length; i++) {
                IOTimeStart = System.currentTimeMillis();
                int start = range[i - 1];
                int end = range[i];
                // System.out.println("Generating W"+i+" From "+start+" to "+end);
                Matrix matrix = new DenseMatrix(end - start, nPCs);
                matrix = PCAUtils.randomMatrix(end - start, nPCs);
                PCAUtils.printMatrixInDenseTextFormat(matrix, outputPath + File.separator + "W" + i);
                
                String Wcommand = "./WSender.sh "+i+" "+line;
                System.out.println(Wcommand);
                p = Runtime.getRuntime().exec(Wcommand);
                //BufferedReader XtXreader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                //String XtXS;
                //while ((XtXS = XtXreader.readLine()) != null) {
                //    System.out.println("Script output: " + XtXS);
                //}

                System.out.println("Called Wsender.sh for index="+i);
                
                IOTimeEnd = System.currentTimeMillis();
                totalIOTime += IOTimeEnd - IOTimeStart;
            }
            String Wcommand = "./doneInitSender.sh "+line;
            System.out.println(Wcommand);
            p = Runtime.getRuntime().exec(Wcommand);
        }
        else{
            System.out.println("Not Selected Root");
        }
        
        
        System.out.println("Generation of Initial W's is complete \nStarting E-M Iterations");
        
        File convergenceCheckFile = new File("converged");
        
        File doneInit = new File("doneInit");
        while(!doneInit.exists()){
            //Thread.sleep(10);
        }
        System.out.println("Initial W's are ready");
	startTime = System.currentTimeMillis();        
        for (int round = 0; round < maxIter; round++) {
            //System.out.println("count: "+vectors.count());
            System.out.println("\n\nStarting Round " + round);
            int [] doneCheck = new int[nodes.length];
            long heapSize1 = Runtime.getRuntime().totalMemory() / 1024 / 1024;
            long heapMaxSize1 = Runtime.getRuntime().maxMemory() / 1024 / 1024;
            long heapFreeSize1 = Runtime.getRuntime().freeMemory() / 1024 / 1024;
            //    System.out.println("HeapFreeSize: " + heapFreeSize1+"\nHeapAllocatedSize: " + (heapSize1-heapFreeSize1));
            // M=W1'*W1;
            Matrix M = new DenseMatrix(nPCs, nPCs);
            
            JavaRDD<Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>> X = null;
            // now for remaining portion of W
            JavaPairRDD<Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>, Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>> YnA = null;
            
            Vector xm_mahout = null;
            boolean timeCheck = false;
            for (int i = 1; i < range.length; i++) {
                
                //*****************************************************************************************************************************************
                //check for convergence
                if ( masterBool && convergenceCheckFile.exists()) {
                    endTime = System.currentTimeMillis();
                    totalTime = endTime - startTime;
                    stat.ppcaIterTime.add((double) totalTime / 1000.0);
                    stat.totalRunTime += (double) totalTime / 1000.0;
                    BufferedReader conv = new BufferedReader(new FileReader(convergenceCheckFile));
                    int convRound = Integer.parseInt(conv.readLine());
                    stat.nIter = convRound+1;
                    for (int j = 0; j < stat.ppcaIterTime.size(); j++) {
                        stat.avgppcaIterTime += stat.ppcaIterTime.get(j);
                    }
                    stat.avgppcaIterTime /= stat.ppcaIterTime.size();
                    stat.IOTime = (double) totalIOTime / 1000.0;
                    
                    // save statistics
                    PCAUtils.printStatToFile(stat, outputPath);
                    int a = convRound+1;
                    System.out.println("Done in "+a+" iterations");
                    return null;
                    //break;
                }
                
                //*****************************************************************************************************************************************
                
                
                //*****************************************************************************************************************************************
                File currentCheckFile = new File("W"+i);
                File masterCheckFile = new File((round-1)+"doneW"+i);
                if(masterBool){
                    while(round != 0 && !masterCheckFile.exists()){
                        //System.out.println((round-1)+"doneW"+i+" Not Exists");
                        if ( convergenceCheckFile.exists()) {
                            endTime = System.currentTimeMillis();
                            totalTime = endTime - startTime;
                            stat.ppcaIterTime.add((double) totalTime / 1000.0);
                            stat.totalRunTime += (double) totalTime / 1000.0;
                            BufferedReader conv = new BufferedReader(new FileReader(convergenceCheckFile));
                            int convRound = Integer.parseInt(conv.readLine());
                            stat.nIter = convRound+1;
                            for (int j = 0; j < stat.ppcaIterTime.size(); j++) {
                                stat.avgppcaIterTime += stat.ppcaIterTime.get(j);
                            }
                            stat.avgppcaIterTime /= stat.ppcaIterTime.size();
                            stat.IOTime = (double) totalIOTime / 1000.0;
                            
                            // save statistics
                            PCAUtils.printStatToFile(stat, outputPath);
                            int a = convRound+1;
                            System.out.println("Done in "+a+" iterations");
                            return null;
                            //break;
                        }
                        //Thread.sleep(100);
                    }
                }
                    
                else{
                    while(!currentCheckFile.exists()){
                        //System.out.println(currentCheckFile.getName()+" Not Exists");
                        if ( convergenceCheckFile.exists()) {
                            endTime = System.currentTimeMillis();
                            totalTime = endTime - startTime;
                            stat.ppcaIterTime.add((double) totalTime / 1000.0);
                            stat.totalRunTime += (double) totalTime / 1000.0;
                            BufferedReader conv = new BufferedReader(new FileReader(convergenceCheckFile));
                            int convRound = Integer.parseInt(conv.readLine());
                            stat.nIter = convRound+1;
                            for (int j = 0; j < stat.ppcaIterTime.size(); j++) {
                                stat.avgppcaIterTime += stat.ppcaIterTime.get(j);
                            }
                            stat.avgppcaIterTime /= stat.ppcaIterTime.size();
                            stat.IOTime = (double) totalIOTime / 1000.0;
                            
                            // save statistics
                            PCAUtils.printStatToFile(stat, outputPath);
                            int a = convRound+1;
                            System.out.println("Done in "+a+" iterations");
                            return null;
                            //break;
                        }
                        //Thread.sleep(100);
                    }
                }
		 //*****************************************************************************************************************************************/
                if(timeCheck == false){
			endTime = System.currentTimeMillis();
            		totalTime = endTime - startTime;
            		System.out.println("Round: "+round);
            		System.out.println((double) totalTime / 1000.0);
			timeCheck = true;
           		startTime = System.currentTimeMillis();
		}
                
                final int start = range[i - 1];
                final int end = range[i];
                Scanner scan = new Scanner(System.in);
//     		int in = scan.nextInt();
                final Matrix matrix = new DenseMatrix(end - start, nPCs);
                IOTimeStart = System.currentTimeMillis();
                PCAUtils.loadMatrixInDenseTextFormat(matrix, outputPath + File.separator + "W" + i);
                if(!masterBool) currentCheckFile.delete();

		IOTimeEnd = System.currentTimeMillis();
                totalIOTime += IOTimeEnd - IOTimeStart;
                
                System.out.println("W" + i + " is Loaded From " + start + " to " + end);
                
                
                // M+=Wi'*Wi;
                M = M.plus(matrix.transpose().times(matrix));
                final Broadcast<Matrix> br_centralC = sc.broadcast(matrix);
                // System.out.println(matrix);
                // Xm = mu*Wi
                xm_mahout = new DenseVector(nPCs);
                xm_mahout = PCAUtils.denseVectorTimesMatrix(meanVector, matrix, start, end, xm_mahout);
                
                // Broadcast Xm becaus`e it will be used in several iterations.
                final Broadcast<Vector> br_xm_mahout = sc.broadcast(xm_mahout);
                
                if (i == 1) {
                    // initializing the very first copy of X
                    // if(round!=0) System.out.println(X.take(100).get(0)._2);
                    
                    X = vectors.map(
                                    new Function<Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>, Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>>() {
                                        
                                        @Override
                                        public Tuple2<Integer, org.apache.spark.mllib.linalg.Vector> call(
                                                                                                          Tuple2<Integer, org.apache.spark.mllib.linalg.Vector> v1) throws Exception {
                                            // TODO Auto-generated method stub
                                            // First Iteration
                                            
                                            org.apache.spark.mllib.linalg.Vector yi = v1._2;
                                            
                                            double resArrayX[] = new double[nPCs];
                                            
                                            // multiplying with W1
                                            Matrix matrix = br_centralC.value();
                                            int matrixCols = matrix.numCols();
                                            int[] indices;
                                            for (int col = 0; col < matrixCols; col++) {
                                                indices = ((SparseVector) yi).indices();
                                                int index = 0, i = 0;
                                                double value = 0;
                                                double dotRes = 0;
                                                for (i = 0; i < indices.length; i++) {
                                                    index = indices[i];
                                                    // checking the range here
                                                    if (index > end) {
                                                        break;
                                                    }
                                                    if (index >= start & index < end) {
                                                        value = yi.apply(index);
                                                        // index-initialStart will give
                                                        // right row
                                                        // index
                                                        dotRes += matrix.getQuick(index - start, col) * value;
                                                    }
                                                    
                                                }
                                                // X=Y*Wi-mu*Wi
                                                resArrayX[col] = dotRes - br_xm_mahout.value().getQuick(col);
                                            }
                                            
                                            Tuple2<Integer, org.apache.spark.mllib.linalg.Vector> xi = new Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>(
                                                                                                                                                                 v1._1, org.apache.spark.mllib.linalg.Vectors.dense(resArrayX));
                                            return xi;
                                        }
                                        
                                    });// TODO
                    // check
                    // if
                    // improves
                    // System.out.println(X.take(100).get(0)._2);
                    int count = (int) X.count();
                    System.out.println("Done Generating X for loop for i=" + i);
                    
                    
                } else {
                    // zipping two RDDs for X=X+Y*Wi
                    // System.out.println("Inside else before Zip "+X.count());
                    YnA = vectors.zip(X);
                    X = null;
                    X = YnA.map(
                                new Function<Tuple2<Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>, Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>>, Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>>() {
                                    
                                    @Override
                                    public Tuple2<Integer, org.apache.spark.mllib.linalg.Vector> call(
                                                                                                      Tuple2<Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>, Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>> v1)
                                    throws Exception {
                                        // TODO Auto-generated method stub
                                        // TODO Auto-generated method stub
                                        // Get Y and X
                                        
                                        org.apache.spark.mllib.linalg.Vector yi = v1._1._2;
                                        org.apache.spark.mllib.linalg.Vector xi = v1._2._2;
                                        
                                        double resArrayX[] = new double[nPCs];
                                        
                                        // Y*Wi
                                        Matrix matrix = br_centralC.value();
                                        // if(v1._2._1.intValue()==0)System.out.println("inside----------\n"+matrix);
                                        int matrixCols = matrix.numCols();
                                        int[] indices;
                                        // range
                                        for (int col = 0; col < matrixCols; col++) {
                                            indices = ((SparseVector) yi).indices();
                                            int index = 0, i = 0;
                                            double value = 0;
                                            double dotRes = 0;
                                            for (i = 0; i < indices.length; i++) {
                                                index = indices[i];
                                                if (index > end) {
                                                    break;
                                                }
                                                // multiply only those within range
                                                if (index >= start & index < end) {
                                                    value = yi.apply(index);
                                                    // Wi's row is not equal to
                                                    // index but
                                                    // index offset by start
                                                    dotRes += matrix.getQuick(index - start, col) * value;
                                                }
                                            }
                                            resArrayX[col] = xi.apply(col) + dotRes - br_xm_mahout.value().getQuick(col);
                                        }
                                        // X=X+Y*Wi-mu*Wi
                                        
                                        // TODO check is zipping with same index
                                        // remove
                                        // later
                                        if (!v1._1._1.equals(v1._2._1))
                                            System.out.println(v1._1._1 + " " + v1._2._1 + "Index Mismatch");
                                        
                                        Tuple2<Integer, org.apache.spark.mllib.linalg.Vector> resXi = new Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>(
                                                                                                                                                                v1._2._1, org.apache.spark.mllib.linalg.Vectors.dense(resArrayX));
                                        
                                        return resXi;
                                    }
                                    
                                });
                    
                    long count = X.count();
                    YnA = null;
                    System.gc();
                    System.runFinalization();
                    System.out.println("Done Generating X for i=" + i);
                    
                    
                }
                
            }
            
            YnA = vectors.zip(X);
            X = null;
            System.gc();
            System.runFinalization();
            System.out.println("After Map after last Zip " + YnA.count());
            
            
            Matrix centralYtX = null;
            Matrix centralXtX = null;
            Vector centralSumX = null;
            // Broadcast Xm because it will be used in several iterations.
            xm_mahout = new DenseVector(nPCs);
            for (int i = 0; i < xm_mahout.size(); i++) {
                xm_mahout.setQuick(i, 0);
            }
            // System.out.println(xm_mahout);
            final Broadcast<Vector> br_xm_mahout = sc.broadcast(xm_mahout);
            
            Matrix invM = PCAUtils.inv(M);
            // Matrix W=new DenseMatrix(nCols, nPCs);//Removed
            Matrix invXtX_central = null;
            // this stage starts after getting complete X
            double maxWnew = 0;
            double dw = 0;
            for (int i = 1; i < range.length; i++) {
                
                final int start = range[i - 1];
                final int end = range[i];
                
                System.out.println("Generating new W" + i+" from "+start+" to "+end);
                
                
                
                // calculate XtX in the first iteration
                if (i == 1) {
                    
                    final Accumulator<double[][]> matrixAccumXtx = sc.accumulator(new double[nPCs][nPCs],
                                                                                  new MatrixAccumulatorParam());
                    final Accumulator<double[]> matrixAccumX = sc.accumulator(new double[nPCs], new VectorAccumulatorParam());
                    
                    final double[][] resArrayXtX = new double[nPCs][nPCs];
                    
                    final double[][] internalSumXtX = new double[nPCs][nPCs];
                    final double[] internalSumX = new double[nPCs];
                    
                    final Accumulator<double[][]> matrixAccumYtx = sc.accumulator(new double[end - start][nPCs],
                                                                                  new MatrixAccumulatorParam());
                    /*
                     * Initialize the output matrices and vectors once in order to
                     * avoid generating massive intermediate data in the workers
                     */
                    final double[][] resArrayYtX = new double[end - start][nPCs];
                    /*
                     * Used to sum the vectors in one partition.
                     */
                    final double[][] internalSumYtX = new double[end - start][nPCs];
                    
                    YnA.foreachPartition(
                                         new VoidFunction<Iterator<Tuple2<Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>, Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>>>>() {
                                             
                                             @Override
                                             public void call(
                                                              Iterator<Tuple2<Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>, Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>>> arg0)
                                             throws Exception {
                                                 // TODO Auto-generated method stub
                                                 org.apache.spark.mllib.linalg.Vector yi;
                                                 org.apache.spark.mllib.linalg.Vector xi;
                                                 Tuple2<Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>, Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>> input = null;
                                                 while (arg0.hasNext()) {
                                                     input = arg0.next();
                                                     yi = input._1._2;
                                                     xi = input._2._2;
                                                     double resArrayX[] = xi.toArray();
                                                     // get only the sparse indices
                                                     int[] indices = ((SparseVector) yi).indices();
                                                     
                                                     PCAUtils.outerProductWithIndices(yi, br_ym_mahout.value(), resArrayX,
                                                                                      br_xm_mahout.value(), resArrayYtX, indices, start, end);
                                                     PCAUtils.outerProductArrayInput(resArrayX, br_xm_mahout.value(), resArrayX,
                                                                                     br_xm_mahout.value(), resArrayXtX);
                                                     int i, j, rowIndexYtX;
                                                     
                                                     // add the sparse indices only
                                                     for (i = 0; i < indices.length; i++) {
                                                         rowIndexYtX = indices[i];
                                                         if (rowIndexYtX > end) {
                                                             break;
                                                         }
                                                         if (rowIndexYtX >= start && rowIndexYtX < end) {
                                                             for (j = 0; j < nPCs; j++) {
                                                                 internalSumYtX[rowIndexYtX - start][j] += resArrayYtX[rowIndexYtX
                                                                                                                       - start][j];
                                                                 resArrayYtX[rowIndexYtX - start][j] = 0; // reset
                                                                 // it
                                                             }
                                                         }
                                                         
                                                     }
                                                     for (i = 0; i < nPCs; i++) {
                                                         internalSumX[i] += resArrayX[i];
                                                         for (j = 0; j < nPCs; j++) {
                                                             internalSumXtX[i][j] += resArrayXtX[i][j];
                                                             resArrayXtX[i][j] = 0; // reset
                                                             // it
                                                         }
                                                         
                                                     }
                                                 }
                                                 matrixAccumX.add(internalSumX);
                                                 matrixAccumXtx.add(internalSumXtX);
                                                 matrixAccumYtx.add(internalSumYtX);
                                                 System.gc();
                                                 System.runFinalization();
                                             }
                                             
                                             
                                         });// end X'X and Y'X Job
                    /*
                     * Get the values of the accumulators.
                     */
                    centralYtX = new DenseMatrix(matrixAccumYtx.value());
                    centralXtX = new DenseMatrix(matrixAccumXtx.value());
                    centralSumX = new DenseVector(matrixAccumX.value());
                    
                    centralXtX = (invM.transpose().times(centralXtX)).times(invM);
                    
                    
                    PCAUtils.printMatrixInDenseTextFormat(centralXtX, outputPath + File.separator + round+"XtX" + myID);
                    
                    
                    //******************************************************************************************************************************************
                    if(!masterBool){
                        String XtXcommand = "./sendXtX.sh "+round+" "+myID+" "+masterID;
                        //String commandString = "./test.sh "+neighbours[i]+" "+myFileName;
                        System.out.println(XtXcommand);
                        p = Runtime.getRuntime().exec(XtXcommand);
                        //BufferedReader XtXreader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        //String XtXS;
                        //while ((XtXS = XtXreader.readLine()) != null) {
                        //    System.out.println("Script output: " + XtXS);
                        //}
                        //    p = Runtime.getRuntime().exec(command);
                        //    p.waitFor();*/
                        System.out.println("Called sendXtX.sh");
                    }
                    else{
                        String XtXcommand = "./accumulateXtX.sh"+ " "+outputPath+ " "+nPCs+" "+round;
                        //String commandString = "./test.sh "+neighbours[i]+" "+myFileName;
                        System.out.println(XtXcommand);
                        p = Runtime.getRuntime().exec(XtXcommand);
                        //BufferedReader XtXreader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        //String XtXS;
                        //while ((XtXS = XtXreader.readLine()) != null) {
                        //    System.out.println("Script output: " + XtXS);
                       // }
                        //    p = Runtime.getRuntime().exec(command);
                        //    p.waitFor();*/
                        System.out.println("Called sendXtX.sh");
                    }
                    /*****************************************************************************************************************************************/
                    
                    // C = (Ye'*X) / SumXtX;
                    //invXtX_central = PCAUtils.inv(centralXtX);
                    
                    
                    
                } else {
                    /*
                     heapSize1 = Runtime.getRuntime().totalMemory() / 1024 / 1024;
                     heapMaxSize1 = Runtime.getRuntime().maxMemory() / 1024 / 1024;
                     heapFreeSize1 = Runtime.getRuntime().freeMemory() / 1024 / 1024;
                     System.out.println("HeapFreeSize: " + heapFreeSize1+"\nHeapAllocatedSize: " + (heapSize1-heapFreeSize1));
                     */    final Accumulator<double[][]> matrixAccumYtx = sc.accumulator(new double[end - start][nPCs],
                                                                                         new MatrixAccumulatorParam());
                    /*
                     * Initialize the output matrices and vectors once in order to
                     * avoid generating massive intermediate data in the workers
                     */
                    final double[][] resArrayYtX = new double[end - start][nPCs];
                    /*
                     * Used to sum the vectors in one partition.
                     */
                    final double[][] internalSumYtX = new double[end - start][nPCs];
                    /*
                     System.out.println("After Declaration");
                     heapSize1 = Runtime.getRuntime().totalMemory() / 1024 / 1024;
                     heapMaxSize1 = Runtime.getRuntime().maxMemory() / 1024 / 1024;
                     heapFreeSize1 = Runtime.getRuntime().freeMemory() / 1024 / 1024;
                     System.out.println("HeapFreeSize: " + heapFreeSize1+"\nHeapAllocatedSize: " + (heapSize1-heapFreeSize1));
                     */    YnA.foreachPartition(
                                                new VoidFunction<Iterator<Tuple2<Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>, Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>>>>() {
                                                    
                                                    @Override
                                                    public void call(
                                                                     Iterator<Tuple2<Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>, Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>>> arg0)
                                                    throws Exception {
                                                        // TODO Auto-generated method stub
                                                        org.apache.spark.mllib.linalg.Vector yi;
                                                        org.apache.spark.mllib.linalg.Vector xi;
                                                        Tuple2<Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>, Tuple2<Integer, org.apache.spark.mllib.linalg.Vector>> input = null;
                                                        while (arg0.hasNext()) {
                                                            input = arg0.next();
                                                            yi = input._1._2;
                                                            xi = input._2._2;
                                                            double resArrayX[] = xi.toArray();
                                                            // get only the sparse indices
                                                            int[] indices = ((SparseVector) yi).indices();
                                                            
                                                            PCAUtils.outerProductWithIndices(yi, br_ym_mahout.value(), resArrayX,
                                                                                             br_xm_mahout.value(), resArrayYtX, indices, start, end);
                                                            int i, j, rowIndexYtX;
                                                            
                                                            // add the sparse indices only
                                                            for (i = 0; i < indices.length; i++) {
                                                                rowIndexYtX = indices[i];
                                                                if (rowIndexYtX > end) {
                                                                    break;
                                                                }
                                                                if (rowIndexYtX >= start && rowIndexYtX < end) {
                                                                    for (j = 0; j < nPCs; j++) {
                                                                        internalSumYtX[rowIndexYtX - start][j] += resArrayYtX[rowIndexYtX
                                                                                                                              - start][j];
                                                                        resArrayYtX[rowIndexYtX - start][j] = 0; // reset
                                                                        // it
                                                                    }
                                                                }
                                                                
                                                            }
                                                        }
                                                        matrixAccumYtx.add(internalSumYtX);
                                                        System.gc();
                                                        System.runFinalization();
                                                    }
                                                    
                                                });// end X'X and Y'X Job
                    //    System.out.println("Before Accumulator");
                    centralYtX = new DenseMatrix(matrixAccumYtx.value());
                    
                    
                    
                    
                }
                
                /*
                 * Mi = (Yi-Ym)' x (Xi-Xm) = Yi' x (Xi-Xm) - Ym' x (Xi-Xm)
                 *
                 * M = Sum(Mi) = Sum(Yi' x (Xi-Xm)) - Ym' x (Sum(Xi)-N*Xm)
                 *
                 * The first part is done in the previous job and the second in
                 * the following method
                 */
                // System.out.println("Updating YtX");
                centralYtX = PCAUtils.updateXtXAndYtx(centralYtX, centralSumX, meanVector, xm_mahout, nRows, start,
                                                      end);
                
                /*
                 * YtX=Y'*X*M-1
                 */
                
                centralYtX = centralYtX.times(invM);
                /*
                 * XtX=(M-1)'*X'*X*M-1
                 */
                
                //    System.out.println(MyIDInt);
                
                File XtXFile = new File("XtXReady");
                while(!XtXFile.exists()){
                    
                }
                centralXtX = new DenseMatrix(nPCs,nPCs);
                PCAUtils.loadMatrixInDenseTextFormat(centralXtX, outputPath + File.separator + "XtX");

                System.out.println(M.getQuick(0,0)+" "+M.getQuick(5,5));
                System.out.println(centralXtX.getQuick(0,0)+" "+centralXtX.getQuick(5,5));
                invXtX_central = PCAUtils.inv(centralXtX);
                final Matrix centralC = centralYtX.times(invXtX_central);
                
                
                
                // TODO
                // stop condition
                // dw = max(max(abs(W-Wnew) / (sqrt(eps)+max(max(abs(Wnew))))));
                
                /*
                 for (int p = 0; p < end - start; p++) {
                 for (int q = 0; q < nPCs; q++) {
                 maxWnew = Math.max(Math.abs(centralC.getQuick(p, q)), maxWnew);
                 }
                 }
                 final Matrix matrix = new DenseMatrix(end - start, nPCs);
                 IOTimeStart = System.currentTimeMillis();
                 PCAUtils.loadMatrixInDenseTextFormat(matrix, outputPath + File.separator + "W" + i);
                 IOTimeEnd = System.currentTimeMillis();
                 totalIOTime += IOTimeEnd - IOTimeStart;
                 
                 for (int p = 0; p < end - start; p++) {
                 for (int q = 0; q < nPCs; q++) {
                 dw = Math.max(Math.abs(matrix.getQuick(p, q) - centralC.getQuick(p, q)), dw);
                 }
                 }
                 */
                IOTimeStart = System.currentTimeMillis();
                if(!masterBool) PCAUtils.printMatrixInDenseTextFormat(centralC, outputPath + File.separator + "nW" + i);
		else PCAUtils.printMatrixInDenseTextFormat(centralC, outputPath + File.separator +myID+"nW" + i);
                IOTimeEnd = System.currentTimeMillis();
                totalIOTime += IOTimeEnd - IOTimeStart;
                System.out.println("New W" + i + " has beens saved");
                
                //******************************************************************************************************************************************
                if(!masterBool){
                    String command = "./rsync_copy_W.sh "+myID+ " "+masterID+" "+i;
                    //String commandString = "./test.sh "+neighbours[i]+" "+myFileName;
                    System.out.println(command);
                    p = Runtime.getRuntime().exec(command);
                    //BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    //String sss;
                    //while ((sss = reader.readLine()) != null) {
                    //    System.out.println("Script output: " + sss);
                    //}
                    //    p = Runtime.getRuntime().exec(command);
                    //    p.waitFor();*/
                    System.out.println("Called Accumulation for W"+i);
                }
                else{
                    String command = "./centralizedAccumulation.sh "+i+ " "+(end-start)+" "+nPCs+" "+round;
                    //String commandString = "./test.sh "+neighbours[i]+" "+myFileName;
                    System.out.println(command);
                    p = Runtime.getRuntime().exec(command);
                    //BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    //String sss;
                    //while ((sss = reader.readLine()) != null) {
                    //    System.out.println("Script output: " + sss);
                    //}
                    //    p = Runtime.getRuntime().exec(command);
                    //    p.waitFor();*/
                    System.out.println("Called Accumulation for W"+i);
                }

                //*****************************************************************************************************************************************/
                if(i == (nC-1)) XtXFile.delete();
	            
    		heapSize1 = Runtime.getRuntime().totalMemory() / 1024 / 1024;
            
		heapMaxSize1 = Runtime.getRuntime().maxMemory() / 1024 / 1024;
                heapFreeSize1 = Runtime.getRuntime().freeMemory() / 1024 / 1024;
                System.out.println("HeapFreeSize: " + heapFreeSize1+"\nHeapAllocatedSize: " + (heapSize1-heapFreeSize1));
                System.gc ();
                System.runFinalization ();
                
                heapSize1 = Runtime.getRuntime().totalMemory() / 1024 / 1024;
                heapMaxSize1 = Runtime.getRuntime().maxMemory() / 1024 / 1024;
                heapFreeSize1 = Runtime.getRuntime().freeMemory() / 1024 / 1024;
                System.out.println("\nHeapFreeSize: " + heapFreeSize1+"\nHeapAllocatedSize: " + (heapSize1-heapFreeSize1));
            }
            
            //double sqrtEps = 2.2204e-16;
            //dw /= (sqrtEps + maxWnew);
            //stat.dwList.add(dw);
            /*
             if (dw <= tolerance || round == maxIter-1) {
             endTime = System.currentTimeMillis();
             totalTime = endTime - startTime;
             stat.ppcaIterTime.add((double) totalTime / 1000.0);
             stat.totalRunTime += (double) totalTime / 1000.0;
             stat.nIter = round+1;
             for (int j = 0; j < stat.ppcaIterTime.size(); j++) {
             stat.avgppcaIterTime += stat.ppcaIterTime.get(j);
             }
             stat.avgppcaIterTime /= stat.ppcaIterTime.size();
             stat.IOTime = (double) totalIOTime / 1000.0;
             
             // save statistics
             PCAUtils.printStatToFile(stat, outputPath);
             int a = round+1;
             System.out.println("Done in "+a+" iterations");
             return null;
             //break;
             }
             */
            //endTime = System.currentTimeMillis();
            //totalTime = endTime - startTime;
            //stat.ppcaIterTime.add((double) totalTime / 1000.0);
            //stat.totalRunTime += (double) totalTime / 1000.0;	
	    //System.out.println(((double) (totalTime / 1000.0)));            
            //startTime = System.currentTimeMillis();
            
            
            
        }
        
        return null;
    }
    
    private static void printLogMessage(String argName) {
        log.error("Missing arguments -D" + argName);
        log.info(
                 "Usage: -Di=<path/to/input/matrix> -Do=<path/to/outputfolder> -Drows=<number of rows> -Dcols=<number of columns> -Dpcs=<number of principal components> [-DerrSampleRate=<Error sampling rate>] [-DmaxIter=<max iterations>] [-DoutFmt=<output format>] [-DComputeProjectedMatrix=<0/1 (compute projected matrix or not)>]");
    }
}





