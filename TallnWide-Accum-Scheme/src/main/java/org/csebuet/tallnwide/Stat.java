package org.csebuet.tallnwide;

import java.util.ArrayList;

public class Stat {
	public String appName=null;
	public String dataSet=null;
	public int nRows=0;
	public int nCols=0;
	public int nPCs=0;
	public int maxMemory=0;
	public int nPartitions=0;
	public int nIter=0;
	public double totalRunTime=0;
	public double preprocessTime=0;
	public double sketchTime=0;
	public double avgppcaIterTime=0;
	public double IOTime=0;
	public ArrayList<Double> ppcaIterTime=new ArrayList<Double>();
	public ArrayList<Double> errorList=new ArrayList<Double>();
	public ArrayList<Double> dwList=new ArrayList<Double>();
	Stat(){
		
	}
	
}
