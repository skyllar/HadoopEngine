package com.hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;

public class JobTrackerServerImpl {

	public static JobTrackerConfiguration jTCnf;

	public static int initialiseVariables(String[] args) {

		int status = 0;
		String jobTrackerConfigFilePath;

		jobTrackerConfigFilePath = args[0];

		// nameNodeConfigFilePath =
		// "/home/apratim/workspace/HDFSCore/jobTracker/jobTrackerConfig";

		File file = new File(jobTrackerConfigFilePath);
		if (file.exists()) {
			try {
				BufferedReader bufferedReader = new BufferedReader(
						new FileReader(file));
				String line;
				jTCnf = new JobTrackerConfiguration();

				// jTCnf.jobIdCounter = 0;
				// jTCnf.taskIdCounter = 0;
				// jTCnf.dataNodeRefernce = "DataNode";
				// jTCnf.nameNodeRefernce = "NameNode";
				// jTCnf.remoteReference = "Namenode";
				// jTCnf.nameNodePort = 5000;
				// jTCnf.nameNodeIP = "localhost";jobTrackerConfigFilePath
				// jTCnf.fileDescriptor = 0;
				// jTCnf.replicationFactor = 3;
				// jTCnf.blockNumber = 0;
				// jTCnf.thresholdTime = 200000;
				// jTCnf.fileToBlocksRegistryDir =
				// "/home/apratim/workspace/HDFSCore/namenode/";
				// jTCnf.fileToBlocksRegistryFileName = "fileBlocksDetails";
				// jTCnf.fileToBlocksRegistryDelimiter = "~";
				// jTCnf.lastBlockNumberFilePath=/home/apratim/workspace/HDFSCore/namenode/lastBlockNumber.txt
				// jTCnf.jobTrackerIP = "localhost";
				// jTCnf.jobTrackerPort = 5001;
				// jTCnf.jobTrackerReference = "JobTracker";

				line = bufferedReader.readLine();
				jTCnf.remoteReference = line.split("=")[1];

				line = bufferedReader.readLine();
				jTCnf.nameNodePort = Integer.parseInt(line.split("=")[1]);

				line = bufferedReader.readLine();
				jTCnf.nameNodeIP = line.split("=")[1];

				line = bufferedReader.readLine();
				jTCnf.fileDescriptor = Integer.parseInt(line.split("=")[1]);

				line = bufferedReader.readLine();
				jTCnf.replicationFactor = Integer.parseInt(line.split("=")[1]);

				line = bufferedReader.readLine();
				jTCnf.blockNumber = Integer.parseInt(line.split("=")[1]);

				line = bufferedReader.readLine();
				jTCnf.thresholdTime = Integer.parseInt(line.split("=")[1]);

				line = bufferedReader.readLine();
				jTCnf.fileToBlocksRegistryDir = line.split("=")[1];

				line = bufferedReader.readLine();
				jTCnf.fileToBlocksRegistryFileName = line.split("=")[1];

				line = bufferedReader.readLine();
				jTCnf.fileToBlocksRegistryDelimiter = line.split("=")[1];

				line = bufferedReader.readLine();
				jTCnf.lastBlockNumberFile = line.split("=")[1];

				line = bufferedReader.readLine();
				jTCnf.jobTrackerIP = line.split("=")[1];

				line = bufferedReader.readLine();
				jTCnf.jobTrackerPort = Integer.parseInt(line.split("=")[1]);

				line = bufferedReader.readLine();
				jTCnf.jobTrackerReference = line.split("=")[1];

				line = bufferedReader.readLine();
				jTCnf.jobIdCounter = Integer.parseInt(line.split("=")[1]);

				line = bufferedReader.readLine();
				jTCnf.taskIdCounter = Integer.parseInt(line.split("=")[1]);

				line = bufferedReader.readLine();
				jTCnf.dataNodeRefernce = line.split("=")[1];

				line = bufferedReader.readLine();
				jTCnf.nameNodeRefernce = line.split("=")[1];

				line = bufferedReader.readLine();
				jTCnf.lastJobNumberFile = line.split("=")[1];
				bufferedReader.close();

				file = new File(jTCnf.lastJobNumberFile);
				if (!file.exists())
					file.createNewFile();
				else {
					try {
						bufferedReader = new BufferedReader(
								new FileReader(file));
						line = bufferedReader.readLine();
						jTCnf.jobIdCounter = Integer
								.parseInt(line.split("=")[1]);
						bufferedReader.close();
					} catch (Exception e) {
						// e.printStackTrace();
						System.out
								.println("LastBlockNumberFile is invalid creating new file with default last block count..");
						file.createNewFile();
					}
				}

				status = 1;
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			status = 0;
		}
		return status;
	}

	public static void main(String args[]) {
		try {
			int status = initialiseVariables(args);
			if (status == 0) {
				System.out
						.println("JobTracker Could not launch, Error Reading Configuration File..");
			} else {

				System.out.println("JobTracker is starting..");

				HDFSMethods hdfsMethods = new HDFSMethods(jTCnf);
				jTCnf.hdfsMethods = hdfsMethods;

				IJobTracker jobTrackerStub = new JobTrackerInterfaceImpl(jTCnf);
				LocateRegistry.createRegistry(jTCnf.jobTrackerPort);
				Naming.rebind("rmi://" + jTCnf.jobTrackerIP + ":"
						+ jTCnf.jobTrackerPort + "/"
						+ jTCnf.jobTrackerReference, jobTrackerStub);
				System.out.println("JobTracker started.");
			}
		} catch (Exception e) {

			e.printStackTrace();
		}
	}
}
