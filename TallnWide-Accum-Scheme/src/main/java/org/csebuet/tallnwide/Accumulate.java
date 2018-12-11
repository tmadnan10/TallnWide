package org.csebuet.tallnwide;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;

public class Accumulate {
    
    static String WIndex, parentID, otherRootNode, myID, dwString, maxWnewString,partitionCountString;
    static double dw, maxWnew;
    static int row,col,round,partitionCount;
    static boolean root, leaf;
    static String[] neighbours;
    static File[] neigbourFiles;
    
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
        
        BufferedReader br = new BufferedReader(new FileReader("myInfo"));
        String thisLine;
        thisLine = br.readLine();
        String [] splitted = thisLine.split("\\s+");
        
        //not a root node as there is a parent
        if (splitted.length>1) {
            parentID = splitted[0];
        }
        
        //root node
        else root = true;
        
        System.out.println("ParentID: "+parentID);
        thisLine = br.readLine();
        splitted = thisLine.split("\\s+");
        
        if (splitted.length == 1) {
            leaf = true;
        }
        
        neighbours = new String[splitted.length-1];
        neigbourFiles = new File[splitted.length-1];
        
        for (int i = 0; i < splitted.length-1; i++) {
            neighbours[i] = splitted[i];
            neigbourFiles[i] = new File("dummy"+splitted[i]+"W"+WIndex);
        }
        
        done = new boolean[splitted.length-1];
        
        if(root){
            thisLine = br.readLine();
            splitted = thisLine.split("\\s+");
            otherRootNode = splitted[1];
            System.out.println("Other Root Node: "+otherRootNode);
        }
        br.close();
        
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
    
    public static void runCommand(String commandString) throws IOException, InterruptedException{
        System.out.println(commandString);
        Process p = Runtime.getRuntime().exec(commandString);
        p.waitFor();
    }
    
    public static void sendWtoDestination(String source, String destination, String WIndex) throws IOException, InterruptedException{
        System.out.println("Sending "+source+"W"+WIndex+" to "+destination);
        String commandString = "./rsync_copy_W.sh "+source+" "+destination+" "+WIndex;
        System.out.println(commandString);
        runCommand(commandString);
        System.out.println(source+"W"+WIndex+" has been sent to "+destination);
    }
    
    
    public static void notifyParent(String myID, String parentID, String WIndex) throws IOException, InterruptedException{
        System.out.println("Notifying to Parent");
        String dumWIndex = "dummy"+myID+"W"+WIndex;
        String commandString = "./notify.sh "+parentID+" "+dumWIndex;
        //System.out.println(commandString);
        runCommand(commandString);
        System.out.println("Done Notifying");
    }
    
    public static void notifyOtherRoot() throws IOException, InterruptedException {
        String rootWIndexString = WIndex+"R";
        String commandString = "cp nW"+WIndex+" nW"+WIndex+"R";
        runCommand(commandString);
        sendWtoDestination(myID,otherRootNode, rootWIndexString);
        commandString = "rm nW"+WIndex+"R";
        runCommand(commandString);
        commandString = "./notifyRoot.sh "+otherRootNode+" "+WIndex;
        runCommand(commandString);

    }
    
    public static void sendAccumulatedWtoNeighbours(String neighbour, String WIndex) throws IOException, InterruptedException{
        String commandString = "./sendAccumW.sh "+neighbour+" "+WIndex+" "+round;
        System.out.println(round);
        runCommand(commandString);
    }
    
    
    
    
    public static void main(String[] args) throws InterruptedException, IOException {
        double tolerance = 0.05;
        readSystemProperty();
        
        
        System.out.println("load the W of this cluster");
        Matrix m = new DenseMatrix(row, col);
        Matrix prev = new DenseMatrix(row, col);
        PCAUtils.loadMatrixInDenseTextFormat(m, "nW"+WIndex);
        PCAUtils.loadMatrixInDenseTextFormat(prev, "W"+WIndex);
        
        initialize();
        
        
        //has no child i.e. a leaf node
        if (leaf) {
            System.out.println("No child... A Leaf Node\nSend the data to the parent node\nGive a notification by deleting dummy file");
            sendWtoDestination(myID,parentID, WIndex);
            notifyParent(myID, parentID, WIndex);
        }
        
        //it has some child
        //first accumulate from them then send to parent
        else {
            System.out.println("This node has some child nodes\nNeed to acculuate the child data while they are available\nChecking for rcvng any notification from any child node");
            
            while(checkDone()){
                for (int i = 0; i < neigbourFiles.length; i++) {
                    if (!done[i]) {
                        File f = neigbourFiles[i];
                        if(f.exists() && !f.isDirectory()) {
                            //file exists so look for next file
                            //System.out.println(f.getName()+" Exists");
                            //Thread.sleep(10);
                        }
                        else {
                            //System.out.println(f.getName()+" Not Exist");
                            //file doesnt exist so W from this child has been reached
                            Matrix m1 = new DenseMatrix(row, col);
                            PCAUtils.loadMatrixInDenseTextFormat(m1, neighbours[i]+"nW"+WIndex);
                            m = m.plus(m1);
                            PCAUtils.printMatrixInDenseTextFormat(m, "nW"+WIndex);
                            done[i] = true;
                            File file = new File(neighbours[i]+"W"+WIndex);
                            file.delete();
                            neigbourFiles[i].createNewFile();
                            //Thread.sleep(3000);
                        }
                    }
                }
            }
            System.out.println("Done accum from all child nodes");
            if (!root) {
                sendWtoDestination(myID,parentID, WIndex);
                notifyParent(myID, parentID, WIndex);
            }
            else {
                System.out.println("This is a Root Node");
                notifyOtherRoot();
                //check if other root has already notified or not
                File rootCheckFile = new File("rootDone"+WIndex);
                while (!rootCheckFile.exists()) {
                    //System.out.println("Other Root Not Done");
                    //Thread.sleep(100);
                }
                rootCheckFile.delete();

                
                Matrix m1 = new DenseMatrix(row, col);
                PCAUtils.loadMatrixInDenseTextFormat(m1, otherRootNode+"nW"+WIndex+"R");
                m = m.plus(m1);
                System.out.println("Done accumulation from other node");
                PCAUtils.printMatrixInDenseTextFormat(m, "W"+WIndex);
                File file = new File(otherRootNode+"nW"+WIndex+"R");
                file.delete();
                
                //done accumulation from all
                //create alldone file
                File currentCheckFile = new File(round+"doneW"+WIndex);
                currentCheckFile.createNewFile();
                
  ///**************************************************************************************************
   
               //check for convergence
                for (int p = 0; p < row; p++) {
                    for (int q = 0; q < col; q++) {
                        maxWnew = Math.max(Math.abs(m.getQuick(p, q)), maxWnew);
                    }
                }
                for (int p = 0; p < row; p++) {
                    for (int q = 0; q < col; q++) {
                        dw = Math.max(Math.abs(prev.getQuick(p, q) - m.getQuick(p, q)), dw);
                    }
                }
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
                strToWrite = myID+"\\n"+partitionCount+"\\n"+maxWnew+"\\n"+dw;
                String commandString = "./write.sh "+strToWrite;
                runCommand(commandString);
 
   
  //**************************************************************************************************/
                
            }
        }
        
        System.out.println("Done Accumulation from all");
        
        
        //redistribution part
        if (!leaf) {
            System.out.println("Starting Redistribution");
            File currentCheckFile = new File(round+"doneW"+WIndex);
            while (!currentCheckFile.exists()) {
  //              System.out.println(round+"done"+ WIndex +" File Not Generated");
                //Thread.sleep(100);
            }
            
	    String accCommand = "./accumulatedWSender.sh "+WIndex+" "+round;
            for (int i = 0; i < neigbourFiles.length; i++) {
		accCommand += " "+neighbours[i];
                //sendAccumulatedWtoNeighbours(neighbours[i],WIndex);
            }
//	    System.out.println(accCommand);
	    runCommand(accCommand);
            System.out.println("W"+WIndex+" Sent to all neighbour");

        }
        
    }
    
}


