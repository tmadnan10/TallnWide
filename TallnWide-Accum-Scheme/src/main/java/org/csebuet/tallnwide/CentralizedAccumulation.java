package org.csebuet.tallnwide;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;

public class CentralizedAccumulation{
    static String WIndex, parentID, otherRootNode, myID, dwString, maxWnewString,partitionCountString;
    static double dw, maxWnew;
    static int row,col,round,partitionCount;
    static boolean root, leaf;
    static String[] nodes;
    static File[] WFiles;
    static boolean [] doneCheck;
    
    
    public static void initialize() throws IOException {
        
        BufferedReader ID = new BufferedReader(new FileReader("ID"));
        myID = ID.readLine();
        partitionCountString = ID.readLine();
        partitionCount = Integer.parseInt(partitionCountString);
        dwString = ID.readLine();
        dw = Double.parseDouble(dwString);
        maxWnewString = ID.readLine();
        maxWnew = Double.parseDouble(maxWnewString);
        ID.close();
        
        BufferedReader br = new BufferedReader(new FileReader("nodes"));
        String thisLine;
        thisLine = br.readLine();
        String [] splitted = thisLine.split("\\s+");
        
        nodes = new String[splitted.length];
        WFiles = new File[splitted.length];
        doneCheck = new boolean[splitted.length];
        for (int i = 0; i < splitted.length; i++) {
            nodes[i] = splitted[i];
            WFiles[i] = new File(splitted[i]+"nW"+WIndex);
        }
        
        br.close();
        
    }
    
    public static void runCommand(String commandString) throws IOException, InterruptedException{
        System.out.println(commandString);
        Process p = Runtime.getRuntime().exec(commandString);
        p.waitFor();
    }
    
    public static void readSystemProperty(){
        System.out.println("getting initial information");
        WIndex = System.getProperty("WIndex");
        String r = System.getProperty("row");
        row = Integer.parseInt(r);
        String c = System.getProperty("column");
        col = Integer.parseInt(c);
        String rd = System.getProperty("round");
        round = Integer.parseInt(rd);
    }
    
    public static void main(String[] args) throws IOException, InterruptedException {
        readSystemProperty();
        initialize();
        Matrix prev = new DenseMatrix(row, col);
        PCAUtils.loadMatrixInDenseTextFormat(prev, "W"+WIndex);
        int nodeCount = nodes.length;
        Matrix centralC = new DenseMatrix(row,col);
        int id = Integer.parseInt(myID);
        //      System.out.println(nodeCount);
        while(nodeCount > 0){
            for(int k = 0; k < nodes.length; k++){
                if(!doneCheck[k]){
                    if (WFiles[k].exists()){
                        System.out.println(WFiles[k].getName()+" exists");
                        Matrix currentCMatrix = new DenseMatrix(row,col);
                        PCAUtils.loadMatrixInDenseTextFormat(currentCMatrix, nodes[k]+"nW" + WIndex);
                        centralC = centralC.plus(currentCMatrix);
                        nodeCount--;
                        doneCheck[k] = true;
                        System.out.println("Done for "+WFiles[k].getName());
                        WFiles[k].delete();
                    }
                }
            }
        }
        System.out.println("Done Accumulating W\n");
        PCAUtils.printMatrixInDenseTextFormat(centralC, "W"+WIndex);
        
        for (int p = 0; p < row; p++) {
            for (int q = 0; q < col; q++) {
                maxWnew = Math.max(Math.abs(centralC.getQuick(p, q)), maxWnew);
            }
        }
        for (int p = 0; p < row; p++) {
            for (int q = 0; q < col; q++) {
                dw = Math.max(Math.abs(prev.getQuick(p, q) - centralC.getQuick(p, q)), dw);
            }
        }
        double tolerance = 0.05;
        if(Integer.parseInt(WIndex) == partitionCount ) { //last segment of W ..... endpoinnt of a round
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
        
        File currentCheckFile = new File(round+"doneW"+WIndex);
        currentCheckFile.createNewFile();
        
        String accCommand = "./accumulatedWSender.sh "+WIndex+" "+round;
        for (int i = 0; i < nodes.length; i++) {
            accCommand += " "+nodes[i];
            //sendAccumulatedWtoNeighbours(neighbours[i],WIndex);
        }
        //        System.out.println(accCommand);
        runCommand(accCommand);
        System.out.println("W"+WIndex+" Sent to all neighbour");

    }
}

