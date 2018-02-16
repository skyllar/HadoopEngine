package com.hdfs;

import java.io.File;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

import com.hdfs.HDFS.BlockReportRequest;
import com.hdfs.HDFS.DataNodeLocation;
import com.hdfs.HDFS.HeartBeatRequest;
import com.hdfs.HDFS.HeartBeatResponse;

public class RepeatTask extends TimerTask {

	DataNodeConfiguration dNCnf;
	INameNode nameNodeStub;

	public RepeatTask(DataNodeConfiguration dataNodeConfig) {
		dNCnf = dataNodeConfig;
	}

	@Override
	public void run() {
		setNameNodeStub();
		heartBeatScheduler();
		blockReportScheduler();

	}

	private List<Integer> getBlockNumbersList() {

		List<Integer> blocksList = new ArrayList<Integer>();
		String fileName;

		File dir = new File(dNCnf.dataNodeBlocksDirectory);

		for (File file : dir.listFiles()) {
			fileName = file.getName();
			try {
				Integer blockId = Integer.parseInt(fileName);
				blocksList.add(blockId);
			} catch (Exception e) {
				// ignore if not block file
			}

		}
		return blocksList;
	}

	private void setNameNodeStub() {

		try {
			nameNodeStub = (INameNode) Naming.lookup("rmi://"
					+ dNCnf.nameNodeIP + ":" + dNCnf.nameNodePort + "/"
					+ dNCnf.nameNodeReference);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}

	public void heartBeatScheduler() {
		try {
			HeartBeatRequest.Builder heartBeatRequest = HeartBeatRequest
					.newBuilder();

			heartBeatRequest.setId(dNCnf.dataNodeId);

			byte[] returnedMessage = nameNodeStub.heartBeat(heartBeatRequest
					.build().toByteArray());

			if (returnedMessage == null) {
				System.out.println("Message is null..........");
			}
			HeartBeatResponse heartBeatResponse = HeartBeatResponse
					.parseFrom(returnedMessage);

			//System.out.println(heartBeatResponse.getStatus());

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void blockReportScheduler() {

		try {

			List<Integer> blocksList = getBlockNumbersList();

			BlockReportRequest.Builder blockReportRequest = BlockReportRequest
					.newBuilder();

			blockReportRequest.setId(dNCnf.dataNodeId);
			blockReportRequest.addAllBlockNumbers(blocksList);

			DataNodeLocation.Builder dataNodeLocationBuilder = DataNodeLocation
					.newBuilder();

			dataNodeLocationBuilder.setIp(dNCnf.dataNodeIP);

			dataNodeLocationBuilder.setPort(dNCnf.dataNodePort);

			blockReportRequest.setLocation(dataNodeLocationBuilder);

			nameNodeStub.blockReport(blockReportRequest.build().toByteArray());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
