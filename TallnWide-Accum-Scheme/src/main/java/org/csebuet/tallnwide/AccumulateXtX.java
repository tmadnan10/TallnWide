package org.csebuet.tallnwide;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import java.lang.InterruptedException;
public class AccumulateXtX {
    static String myID;
    static File[] neigbourFiles;

    public static void main(String[] args) throws IOException, InterruptedException {
        String outputPath = System.getProperty("Path");
        String PCs = System.getProperty("nPCs");
        int nPCs = Integer.parseInt(PCs);
	String round = System.getProperty("round");
        //String PCs = System.getProperty("round");
        BufferedReader ID = new BufferedReader(new FileReader("ID"));
        myID = ID.readLine();
        
        
        BufferedReader nodeList = new BufferedReader(new FileReader("nodes"));
        String line = nodeList.readLine();
        String [] splittednodes = line.split("\\s+");
        int [] nodes = new int[splittednodes.length];
        File [] XtXFiles = new File[splittednodes.length];
        boolean [] doneCheck = new boolean[splittednodes.length];
        for (int i = 0; i < splittednodes.length; i++) {
            nodes[i] = Integer.parseInt(splittednodes[i]);
            System.out.println(nodes[i]);
            XtXFiles[i] = new File(round+"XtX"+splittednodes[i]);
        }
        //    System.out.println(MyIDInt);
        int nodeCount = nodes.length;
        Matrix centralXtX = new DenseMatrix(nPCs,nPCs);
        int id = Integer.parseInt(myID);
        //      System.out.println(nodeCount);
        while(nodeCount > 0){
            for(int k = 0; k < nodes.length; k++){
                if(!doneCheck[k]){
                    if (XtXFiles[k].exists()){
                        System.out.println(XtXFiles[k].getName()+" exists");
                        Matrix currentXtXMatrix = new DenseMatrix(nPCs,nPCs);
                        PCAUtils.loadMatrixInDenseTextFormat(currentXtXMatrix, outputPath + File.separator + XtXFiles[k].getName());
                        centralXtX = centralXtX.plus(currentXtXMatrix);
                        System.out.println(centralXtX.getQuick(0,0)+" "+centralXtX.getQuick(5,5));
			nodeCount--;
                        doneCheck[k] = true;
                        System.out.println("Done for "+XtXFiles[k].getName());
//                        XtXFiles[k].delete();
                    }
                }
            }
        }
        PCAUtils.printMatrixInDenseTextFormat(centralXtX, outputPath + File.separator + "XtX");
        String command = "./accumulatedXtXSender.sh " + line;
	File f = new File("XtXReady");
	f.createNewFile();
	System.out.println("Done XtXReady");
        System.out.println(command);
        Process p = Runtime.getRuntime().exec(command);
	p.waitFor();
    }
}

