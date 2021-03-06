package com.hdfs;

import java.rmi.Naming;

public class ClientMain {

	static ClientConfiguration clientConf;

	private static void initialiseVariables() {
		try {
			clientConf = new ClientConfiguration();
			clientConf.nameNodeIP = "localhost";
			clientConf.nameNodePort = 5000;
			clientConf.nameNodeRefernce = "Namenode";
			clientConf.dataNodeRefernce = "Datanode";
			clientConf.blockSize = 8;
			clientConf.fileLocationToWriteFrom = "./FilesToWrite";
			clientConf.jobStatusTimerDelay = 3000;

			clientConf.jobTrackerIP = "localhost";
			clientConf.jobTrackerPort = 6000;
			clientConf.jobTrackerRefernce = "Jobtracker";

			clientConf.nameNodeStub = (INameNode) Naming.lookup("rmi://"
					+ clientConf.nameNodeIP + ":" + clientConf.nameNodePort
					+ "/" + clientConf.nameNodeRefernce);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {

		initialiseVariables();
		ClientMethods clientMethods = new ClientMethods(clientConf);

		System.out.println("...Client Program Starting...");
		//
		// if (args.length != 5) {
		// System.out
		// .println("Input Arguments should be: <mapName> <reducerName> <inputFile in HDFS> <outputFile in HDFS> <numReducers> ");
		// return;
		// }

		args = new String[5];
		args[0] = "com.mapper.MapTask";
		args[1] = "com.reducer.ReduceTask";
		args[2] = "b";// fileName;
		args[3] = args[2] + "Output";
		args[4] = "2";

		int jobId = clientMethods.submitMapReduceTask(args);
		// clientMethods.readRemoteFile("aOutput_4_1");
		// clientMethods.readRemoteFile("aOutput_4_2");
		// clientMethods.writeRemoteFile("bOutputFileInHDFS_9_2");

		// clientMethods.writeRemoteFile("a");

		System.out.println("....Client Program Completed...");

	}
}
