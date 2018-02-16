package com.hdfs;

import java.util.ArrayList;

public class ReducerTaskDetail {
	int jobId;
	// int reducerId;
	boolean taskCompleted;
	ArrayList<String> mapOutputFilesToBeReduced = new ArrayList<String>();
	String outputFile;
	String reducerName;
}
