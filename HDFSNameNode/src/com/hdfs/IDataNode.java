package com.hdfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IDataNode extends Remote {

	/* ReadBlockResponse readBlock(ReadBlockRequest)) */
	/* Method to read data from any block given block-number */
	byte[] readBlock(byte[] input) throws RemoteException;

	/* WriteBlockResponse writeBlock(WriteBlockRequest) */
	/* Method to write data to a specific block */
	byte[] writeBlock(byte[] input) throws RemoteException;
}