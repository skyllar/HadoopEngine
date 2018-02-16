package com.hdfs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;

import com.google.protobuf.ByteString;
import com.hdfs.HDFS.BlockLocations;
import com.hdfs.HDFS.DataNodeLocation;
import com.hdfs.HDFS.ReadBlockRequest;
import com.hdfs.HDFS.ReadBlockResponse;
import com.hdfs.HDFS.WriteBlockRequest;
import com.hdfs.HDFS.WriteBlockResponse;

public class DataNodeInterfaceImpl extends UnicastRemoteObject implements
		IDataNode {

	public static DataNodeConfiguration dNCnf;

	protected DataNodeInterfaceImpl(DataNodeConfiguration dataNodeConfiguration)
			throws RemoteException {
		super();
		dNCnf = dataNodeConfiguration;
	}

	@Override
	public byte[] readBlock(byte[] input) {
		byte[] outputData = null;
		try {
			ReadBlockRequest readBlockRequest = ReadBlockRequest
					.parseFrom(input);
			ReadBlockResponse.Builder readBlockResponseBuilder = ReadBlockResponse
					.newBuilder();

			int blockNumber = readBlockRequest.getBlockNumber();

			File file = new File(dNCnf.dataNodeBlocksDirectory + blockNumber);

			if (file.exists()) {
				FileInputStream fileInputStream = new FileInputStream(file);
				byte[] fileData = new byte[(int) file.length()];
				fileInputStream.read(fileData);
				readBlockResponseBuilder.addData(ByteString.copyFrom(fileData));
				fileInputStream.close();

				// 1 for success
				readBlockResponseBuilder.setStatus(1);
			} else {
				// 0 for failure
				readBlockResponseBuilder.setStatus(0);
			}

			outputData = readBlockResponseBuilder.build().toByteArray();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return outputData;
	}

	@Override
	public byte[] writeBlock(byte[] input) {

		byte[] outputData = null;

		try {
			WriteBlockRequest writeBlockRequest = WriteBlockRequest
					.parseFrom(input);
			WriteBlockResponse.Builder writeBlockResponseBuilder = WriteBlockResponse
					.newBuilder();

			int blockNumber = writeBlockRequest.getBlockInfo().getBlockNumber();

			System.out.println("Writing block " + blockNumber + "...");
			File file = new File(dNCnf.dataNodeBlocksDirectory + blockNumber);
			file.createNewFile();
			BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(
					file));

			for (ByteString data : writeBlockRequest.getDataList()) {
				bufferedWriter.write(data.toStringUtf8());
			}
			bufferedWriter.close();
			writeBlockResponseBuilder.setStatus(1);

			DataNodeLocation dataNodeLocation;
			// System.out.println("************"
			// + writeBlockRequest.getBlockInfo().getLocationsList()
			// + "*******");
			if (writeBlockRequest.getBlockInfo().getLocationsList().size() > 0) {
				System.out.println("Datanode sending to another datanode.....");
				dataNodeLocation = writeBlockRequest.getBlockInfo()
						.getLocationsList().get(0);

				List<DataNodeLocation> newDataNodeLocations = writeBlockRequest
						.getBlockInfo()
						.getLocationsList()
						.subList(
								1,
								writeBlockRequest.getBlockInfo()
										.getLocationsList().size());

				WriteBlockRequest.Builder newWriteBlockRequestBuilder = WriteBlockRequest
						.newBuilder();

				newWriteBlockRequestBuilder.addAllData(writeBlockRequest
						.getDataList());
				BlockLocations.Builder newBlockLocationBuilder = BlockLocations
						.newBuilder();
				newBlockLocationBuilder.setBlockNumber(blockNumber);
				newBlockLocationBuilder.addAllLocations(newDataNodeLocations);
				newWriteBlockRequestBuilder
						.setBlockInfo(newBlockLocationBuilder);

				IDataNode dataNodeStub = (IDataNode) Naming.lookup("rmi://"
						+ dataNodeLocation.getIp() + ":"
						+ dataNodeLocation.getPort() + "/"
						+ dNCnf.dataNodeReference);

				byte[] dataToForward = newWriteBlockRequestBuilder.build()
						.toByteArray();
				dataNodeStub.writeBlock(dataToForward);
			}

			outputData = writeBlockResponseBuilder.build().toByteArray();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return outputData;
	}
}