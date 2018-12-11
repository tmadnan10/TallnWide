package org.csebuet.tallnwide;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class FileFormat {
	 private final static Logger log = LoggerFactory.getLogger(FileFormat.class);
	 public enum OutputFormat {
		DENSE,  //Dense matrix 
		LIL, //List of lists
		COO, //Coordinate List
	 } 
	 public enum InputFormat {
		DENSE,
		COO
	 } 
	 public static void main(String[] args) {
			try {
		    System.setOut(new PrintStream(new File("/Users/tariqadnan/Desktop/Working Data Sets/Output/output-file.txt")));
		} catch (Exception e) {
		     e.printStackTrace();
		}
		final String inputPath;
		final int cardinality;
		final String outputPath;
		final InputFormat inputFormat;
		final int maximumRow;
		try {
			inputPath=System.getProperty("Input");
			if(inputPath==null)
				throw new IllegalArgumentException();
		}
		catch(Exception e) {
			printLogMessage("Input");
			return;
		}
		try {
			inputFormat=InputFormat.valueOf(System.getProperty("InputFmt"));
		}
		catch(IllegalArgumentException e) {
		    	 log.warn("Invalid Format " + System.getProperty("InputFmt") );
		    	 return;
		}
		catch(Exception e) {
		    	printLogMessage("InputFmt");
		    	return;
		}
		try {
			outputPath=System.getProperty("Output");
			if(outputPath==null)
				throw new IllegalArgumentException();
			File outputFile=new File(outputPath);
			if( outputFile.isFile() || outputFile==null )
			{
				log.error("Output Path must be a directory, " + outputPath + " is either not a directory or not a valid path");
				return;
			}
		}
		catch(Exception e) {
			printLogMessage("Output");
			return;
		}
		try {
			maximumRow=Integer.parseInt(System.getProperty("MaxRow"));
		}
		
		catch(Exception e) {
			printLogMessage("MaxRow");
			return;
		}
		try {
			cardinality=Integer.parseInt(System.getProperty("Cardinality"));
		}
		catch(Exception e) {
			printLogMessage("Cardinality");
			return;
		}
		int base=-1;
		try {
			base=Integer.parseInt(System.getProperty("Base"));
		}
		catch(Exception e) {
			log.warn("It is not specified whether the input is zero-based or one-based, this parameter is useful only if the input is in COO format");
		}
		int initrow = 1000;
		int initcol = 100;
		int flag = 1;
		int row, col;
		switch(inputFormat)
		{
		
			case COO:
				if(base==-1) {
					log.error("You have to specify whether the rows and columns IDs start with 0 or 1 using the argument -DBase");
					return;
				}
				
				
//				for (int i = 1; i <= 3 && flag == 1; ++i)
//				{
//					row = initrow;
//					for (int j = 0; j < i; ++j)
//					{
//						row = row*10;
//					}
//					col = initcol;
//					while(col < row) {
//						col = col *10;
//						convertFromCooToSeq(inputPath,col,base,outputPath,row);
//						//printf("-DInput=%c/Users/tariqadnan/Desktop/Working Data Sets/Input%c -DInputFmt=COO -DOutput=%c/Users/tariqadnan/Desktop/Working Data Sets/Output%c -DMaxRow=%d -DCardinality=%d -DBase=0\n\n\n", '\"', '\"', '\"', '\"', row,col);
//					}
//				}
//				convertFromCooToSeq(inputPath,100000,base,outputPath,10000000);
				convertFromCooToSeq(inputPath,20000000,base,outputPath,40103210);
				//convertFromCooToSeq(inputPath,5000000,base,outputPath,26797591);
				break;
			case DENSE:
				convertFromDenseToSeq(inputPath,cardinality,outputPath);
				break;
		}
		
		
	}
	public static void convertFromDenseToSeq(String inputPath, int cardinality, String outputFolderPath)
	{
		try
    	{
	    	 final Configuration conf = new Configuration();
	         final FileSystem fs = FileSystem.get(conf);
	         SequenceFile.Writer writer;
	
	         final IntWritable key = new IntWritable();
	         final VectorWritable value = new VectorWritable();
	         
	         int lineNumber=0;
	         String thisLine;
	         File[] filePathList=null;
	         File inputFile=new File(inputPath);
	          if(inputFile.isFile()) // if it is a file
	          { 
	        	  filePathList= new File[1];
	        	  filePathList[0]=inputFile;
	          }
	          else
	          {
	        	  filePathList=inputFile.listFiles();
	          }
	          if(filePathList==null)
	          {
	        	  log.error("The path " + inputPath + " does not exist");
	          	  return;
	          }
	          for(File file:filePathList)
	          {
		          BufferedReader br = new BufferedReader(new FileReader(file));
		          Vector vector = null;
		          String outputFileName=outputFolderPath+ File.separator + file.getName() + ".seq";
		          writer=SequenceFile.createWriter(fs, conf, new Path(outputFileName), IntWritable.class, VectorWritable.class, CompressionType.BLOCK);
		          while ((thisLine = br.readLine()) != null) { // while loop begins here
		              if(thisLine.isEmpty())
		            	  continue;
		        	  String [] splitted = thisLine.split("\\s+");
		        	  vector = new SequentialAccessSparseVector(splitted.length);
		        	  for (int i=0; i < splitted.length; i++)
		        	  {
		        		  vector.set(i, Double.parseDouble(splitted[i]));
		        	  }
		        	  key.set(lineNumber);
		        	  value.set(vector);
		        	  //System.out.println(vector);
		        	  writer.append(key,value);//write last row
		        	  lineNumber++;
		          }
		          writer.close();
	          }   
		    }
	    	catch (Exception e) {
	    		e.printStackTrace();
	    	}
		
	    	
	}
	public static void convertFromCooToSeq(String inputPath, int cardinality, int base, String outputFolderPath, int maximumRow){
    
		
		
		
		try
    	{
    	 final Configuration conf = new Configuration();
         final FileSystem fs = FileSystem.get(conf);
         SequenceFile.Writer writer=null;

         final IntWritable key = new IntWritable();
         final VectorWritable value = new VectorWritable();
         
         Vector vector = null;
    
          String thisLine;
          
          int lineNumber=0;
          int prevRowID=-1;
          boolean first=true;
          int maxRow = maximumRow;
          int maxCol = cardinality;
          System.out.println(maximumRow + " "+ cardinality);
          File[] filePathList=null;
	      File inputFile=new File(inputPath);
          if(inputFile.isFile()) // if it is a file
          { 
        	  filePathList= new File[1];
        	  filePathList[0]=inputFile;
          }
          else
          {
        	  filePathList=inputFile.listFiles();
          }
          if(filePathList==null)
          {
        	  log.error("The path " + inputPath + " does not exist");
          	  return;
          }
          int count=0;
          int colCount = 0;
          for(File file:filePathList)
          {
        	  	
        	  	BufferedReader br = new BufferedReader(new FileReader(file));
        	  	String outputFileName=outputFolderPath+ File.separator + file.getName() + maxRow + "x" + maxCol +".seq";
	          writer=SequenceFile.createWriter(fs, conf, new Path(outputFileName), IntWritable.class, VectorWritable.class, CompressionType.BLOCK);
	          
	          while ((thisLine = br.readLine()) != null) { // while loop begins here   		   
	        	  	String [] splitted = thisLine.split("\\s+");
	        	  	int rowID=Integer.parseInt(splitted[0])-12;
	        	  	int colID=Integer.parseInt(splitted[1])-12;
	        	  	double element=1;//Double.parseDouble(splitted[2]);
	        	  	//if(count==maxRow) break;//take first 5000 count line number
	        	  	if(colID>maxCol) continue;//take 5000 columns
	        	  	if(first)
	        	  	{
	        		  first=false;
	        		  vector = new SequentialAccessSparseVector(cardinality+1);
	        	  	}
	        	  	else if(rowID != prevRowID)
	        	  	{
	        		  key.set(prevRowID);
	        		  value.set(vector);
	            	  //System.out.println(vector);
	        		  count++;
	            	  writer.append(key,value);//write last row
	            	  System.out.println("written row no: "+count+" key: "+key+" value: ");
	            	  vector = new SequentialAccessSparseVector(cardinality+1);
	        	  	}
	        	  	colCount++;
	        	  	prevRowID=rowID;
	        	  	vector.set(colID-base,element);
	          }
	          //System.out.println(count+" "+key);
          }
          //writer.close();
          
          if(writer!=null) //append last vector in last file
          {
	         key.set(prevRowID);
	         value.set(vector);
	    	  	 writer.append(key,value);//write last row
	         writer.close();
	         System.out.println("written row no: "+count+" key: "+key);
	         System.out.println("Avg Cols: " + 1.0*colCount/count);
          }
          
          System.out.println("\n\n\n\n");
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    	}
    }
	private static void printLogMessage(String argName )
	 {
		log.error("Missing arguments -D" + argName);
		log.info("Usage: -DInput=<path/to/input/matrix> -DOutput=<path/to/outputfolder> -DInputFmt=<DENSE/COO> -DCardinaality=<number of columns> [-DBase=<0/1>(0 if input is zero-based, 1 if input is 1-based]"); 
	 }
	
}
