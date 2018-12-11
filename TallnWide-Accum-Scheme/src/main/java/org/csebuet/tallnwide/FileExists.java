package org.csebuet.tallnwide;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.THEAD;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.spark.sql.execution.command.LoadDataCommand;
import org.netlib.util.booleanW;
import org.netlib.util.doubleW;


public class FileExists {
	static boolean[] done = null;
	static boolean checkDone(){
		for (int i = 0; i < done.length; i++) {
			if (!done[i]) {
				return true;
			}
		}
		System.out.println("Done");
		return false;
	}
	
	static double[][] loadData(int row, int col, String path) throws IOException{
		double[][] m = new double[row][col];
		BufferedReader W = new BufferedReader(new FileReader(path));
		String inputW;
		for (int i = 0; i < row; i++) {
			inputW = W.readLine();
			String[] sl = inputW.split("\\s+");
			for (int j = 0; j < sl.length; j++) {
				m[i][j] = Double.parseDouble(sl[j]);
			}
		}
		return m;
	}
	
	static void saveData(int row, int col, double [][] data, String path) throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter(path));
		
		for (int i = 0; i < row; i++) {
			String a = "";
			for (int j = 0; j < col; j++) {
				a += data[i][j]+" ";
			}
			bw.write(a+"\n");
		}
		bw.flush();
		bw.close();
	}
	
	public static void main(String[] args) throws InterruptedException, IOException {
		String myFileName = System.getProperty("fileIndex");
		String r = System.getProperty("row");
		int row = Integer.parseInt(r);
		String c = System.getProperty("column");
		int col = Integer.parseInt(c);
		File myFile = new File("W"+myFileName+".txt");
		//Matrix m = new DenseMatrix(5, 5);
		double[][] m = loadData(row, col, "W"+myFileName+".txt")	;
		//PCAUtils.loadMatrixInDenseTextFormat(m, );
		boolean root = false;
		String parentID = null;
		String otherRootNode = null;
		String[] neighbours = null;
		File[] neigbourFiles = null;
		BufferedReader br = new BufferedReader(new FileReader("myInfo.txt"));
		String thisLine;
		thisLine = br.readLine();
		String [] splitted = thisLine.split("\\s+");
		if (splitted.length>1) {
			parentID = splitted[0];
		}
		else root = true;
		
		thisLine = br.readLine();
		splitted = thisLine.split("\\s+");
		//it has some child
		if (splitted.length > 1) {
			neighbours = new String[splitted.length-1];
			neigbourFiles = new File[splitted.length-1];
			for (int i = 0; i < splitted.length-1; i++) {
				neighbours[i] = splitted[i];
				neigbourFiles[i] = new File("dummy"+splitted[i]+"W"+myFileName);
			}
			
			done = new boolean[splitted.length-1];
			
			
			
			while(checkDone()){
				for (int i = 0; i < neigbourFiles.length; i++) {
					if (!done[i]) {
						File f = neigbourFiles[i];
						if(f.exists() && !f.isDirectory()) { 
						    System.out.println(f.getName()+" Exists");
						    Thread.sleep(4000);
						}
						else {
							System.out.println(f.getName()+" Not Exist");
							Process p;
							String commandString = "./test.sh "+neighbours[i]+" "+myFileName;
							p = Runtime.getRuntime().exec(commandString);
							p.waitFor();
							System.out.println("done comm");
	
							
							double[][] m1 = loadData(row, col, neighbours[i]+"W"+myFileName+".txt");
							

							for (int j = 0; j < row; j++) {
								for (int k = 0; k < col; k++){
									m[j][k] += m1[j][k];
									//m.set(j, k, m.get(j, k)+m1.get(j, k));
								}
							}
							
							saveData(row, col, m, "W"+myFileName+".txt");
							done[i] = true;
							File file = new File(neighbours[i]+"W"+myFileName+".txt");
							file.delete();
							neigbourFiles[i].createNewFile();
							Thread.sleep(3000);
						}
					}
				}
			}
			System.out.println("Done accum from child");
		}
		//has no child i.e. a leaf node
		else {
			System.out.println("No child... A Leaf Node");
		}
		
		//go and announce to parent if there is any;
		if (parentID != null) {
			Process p;
			BufferedReader ID = new BufferedReader(new FileReader("ID.txt"));
			thisLine = ID.readLine();
			String dummyFileName = "dummy"+thisLine+"W"+myFileName;
			String commandString = "./notify.sh "+parentID+" "+dummyFileName;
			p = Runtime.getRuntime().exec(commandString);
			p.waitFor();
			System.out.println("Done Notifying");
		}
		else {
			System.out.println("Root Node");
			thisLine = br.readLine();
			splitted = thisLine.split("\\s+");
			otherRootNode = splitted[1];
			String commandString = "./notifyRoot.sh "+otherRootNode+" "+myFileName;
			Process p;
			p = Runtime.getRuntime().exec(commandString);
			p.waitFor();
			//check if other root has already notified or not
			File rootCheckFile = new File("rootDone");
			while (!rootCheckFile.exists()) {
				Thread.sleep(4000);
			}
			//get the other root file
			commandString = "./test.sh "+otherRootNode+" "+myFileName+".txt";
			p = Runtime.getRuntime().exec(commandString);
			p.waitFor();
			double[][] m1 = loadData(row, col, otherRootNode+"W"+myFileName);
			for (int j = 0; j < row; j++) {
				for (int k = 0; k < col; k++){
					m[j][k] += m1[j][k];
					//m.set(j, k, m.get(j, k)+m1.get(j, k));
				}
			}
			saveData(row, col, m, "W"+myFileName);
			//done accumulation from all
			//create alldone file
			File currentCheckFile = new File("doneW"+myFileName);
			currentCheckFile.createNewFile();
		}
		if (neighbours.length > 0) {
			File currentCheckFile = new File("doneW"+myFileName);
			while (!currentCheckFile.exists()) {
				System.out.println("done"+ myFileName +" File Not Generated");
				Thread.sleep(4000);
			}
			Process p;
			for (int i = 0; i < neigbourFiles.length; i++) {
				String commandString = "./sendAccumW.sh "+neighbours[i]+" "+myFileName;
				System.out.println(commandString);
				p = Runtime.getRuntime().exec(commandString);
			}
			System.out.println("W"+myFileName+" Sent to all neighbour");
			
			
		}
		
		
		
	}
}
