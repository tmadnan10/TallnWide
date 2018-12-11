package org.csebuet.tallnwide;

import java.util.ArrayList;

public class AccumulationStat {
	public String appName=null;
	public String dataSet=null;
	public int nRows=0;
	public int nCols=0;
	public int nPCs=0;
	public int nPartitions=0;
	public int nIter=0;
	public double totalAccumulationTime=0;
	public double avgTime=0;
	public double IOTime=0;
	public ArrayList<Double> ppcaIterTime=new ArrayList<Double>();
	public ArrayList<Double> errorList=new ArrayList<Double>();
	public ArrayList<Double> dwList=new ArrayList<Double>();
	AccumulationStat(){
		
	}
	
}
