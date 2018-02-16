package com.hdfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IJobTracker extends Remote {

	/* JobSubmitResponse jobSubmit(JobSubmitRequest) */
	byte[] jobSubmit(byte[] input) throws RemoteException;

	/* JobStatusResponse getJobStatus(JobStatusRequest) */
	byte[] getJobStatus(byte[] input) throws RemoteException;

	/* HeartBeatResponse heartBeat(HeartBeatRequest) */
	byte[] heartBeat(byte[] input) throws RemoteException;
}
