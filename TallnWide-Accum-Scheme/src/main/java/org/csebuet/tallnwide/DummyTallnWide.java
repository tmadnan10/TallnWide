package org.csebuet.tallnwide;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class DummyTallnWide {
	static int partitionCount;
	static int D, d, round;

	public static void main(String[] args) throws InterruptedException, IOException {
		System.out.println("Hello World!");
		partitionCount =  Integer.parseInt(System.getProperty("partitionCount"));
		D =  Integer.parseInt(System.getProperty("D"));
		d =  Integer.parseInt(System.getProperty("d"));
		round =  Integer.parseInt(System.getProperty("round"));
		Process p1;
		p1 = Runtime.getRuntime().exec("./deleteDummy.sh");
		for (int k = 0; k < partitionCount; k++) {
			File currentCheckFile = new File("doneW"+k);
			while(round != 1 && !currentCheckFile.exists()){
				System.out.println("doneW"+k+" Not Exists");
				Thread.sleep(4000);
			}
			currentCheckFile.delete();
			BufferedReader br = new BufferedReader(new FileReader("myInfo"));
			String thisLine;
			thisLine = br.readLine();
			thisLine = br.readLine();
			String[] splitted = thisLine.split("\\s+");
			File[] neigbourFiles = null;
			if (splitted.length > 1) {
				neigbourFiles = new File[splitted.length-1];
				for (int i = 0; i < splitted.length-1; i++) {
					neigbourFiles[i] = new File("dummy"+splitted[i]+"W"+k);
					neigbourFiles[i].createNewFile();
				}
			}
			
			BufferedWriter bw = new BufferedWriter(new FileWriter("W"+k));
			for (int i = 0; i < D; i++) {
				String line = "";
				for (int j = 0; j < d-1; j++) {
					line += "1 ";
					Thread.sleep(1);
				}
				line += "1\n";
				bw.write(line);
			}
			bw.flush();
			bw.close();
			String command = "./accumulate.sh "+k+ " "+D+" "+d;
			Process p;
			//String commandString = "./test.sh "+neighbours[i]+" "+myFileName;
			System.out.println(command);
			p = Runtime.getRuntime().exec(command);
			p.waitFor();
			System.out.println("Called Accumulation for W"+k);
			
		}
		
		System.out.println("Hello World");

	}

}
