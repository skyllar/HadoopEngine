package com.hdfs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.LinkedHashMap;

public class MapReduceTaskWithThreadPool {

	TaskTrackerConfiguration tTCnf;

	ThreadPool mapperPool;
	ThreadPool reducerPool;

	LinkedHashMap<Integer, MapperTaskDetail> mapRunningMapperIdToTaskInfo;
	LinkedHashMap<Integer, ReducerTaskDetail> mapRunningReducerIdToTaskInfo;

	public MapReduceTaskWithThreadPool(
			TaskTrackerConfiguration tTCnf,
			LinkedHashMap<Integer, MapperTaskDetail> mapRunningMapperIdToTaskInfo,
			LinkedHashMap<Integer, ReducerTaskDetail> mapRunningReducerIdToTaskInfo) {
		this.mapRunningMapperIdToTaskInfo = mapRunningMapperIdToTaskInfo;
		this.mapRunningReducerIdToTaskInfo = mapRunningReducerIdToTaskInfo;
		this.tTCnf = tTCnf;
	}

	public void submitMapTask(int taskId) {
		mapperPool.addTask(new MapTask(taskId));

	}

	public void submitReduceTask(int taskId) {
		reducerPool.addTask(new ReduceTask(taskId));
	}

	public void createMapperThreadPool(int threadCount) {
		ThreadPool mapperPool = new ThreadPool(threadCount);
		this.mapperPool = mapperPool;
		System.out
				.println("----------------Map Task Thread Pool Created-----------------");
	}

	public void createReducerThreadPool(int threadCount) {
		ThreadPool reducerPool = new ThreadPool(threadCount);
		this.reducerPool = reducerPool;
		System.out
				.println("----------------Reduce Task Thread Pool Created-----------------");
	}

	public void shutDownThreadPool() {
		mapperPool.shutdown();
		reducerPool.shutdown();
	}

	private class MapTask implements Runnable {
		int taskId;

		public MapTask(int taskId) {
			this.taskId = taskId;
		}

		@Override
		public void run() {
			performMapTask();
		}

		private void performMapTask() {
			System.out.println("*********Map Task " + taskId
					+ " Started************");

			HDFSMethods hdfsMethods = new HDFSMethods(tTCnf);

			Integer blockNumber = mapRunningMapperIdToTaskInfo.get(taskId).blockNumberToBeMapped;
			String dataNodeIP = mapRunningMapperIdToTaskInfo.get(taskId).blockDatanodeIp;
			Integer dataNodePort = mapRunningMapperIdToTaskInfo.get(taskId).blockDatanodePort;
			String mapName = mapRunningMapperIdToTaskInfo.get(taskId).mapName;

			String blockData = hdfsMethods.readBlockFromDatanode(blockNumber,
					dataNodeIP, dataNodePort);

			// *****************************************perform
			// mapping*************************************************
			String mapOuputContent = startMapping(mapName, blockData);

			String mapOutputFileName = "job_"
					+ mapRunningMapperIdToTaskInfo.get(taskId).jobId + "_map_"
					+ taskId;

			System.out.println("write map output file: " + mapOutputFileName);

			try {
				BufferedWriter bufferedWriter = new BufferedWriter(
						new FileWriter(new File(tTCnf.mappedFilesLocation
								+ "\\" + mapOutputFileName)));
				bufferedWriter.write(mapOuputContent);
				bufferedWriter.close();

			} catch (IOException e) {
				e.printStackTrace();
			}

			// write file in hdfs
			hdfsMethods.writeRemoteFile(mapOutputFileName,
					tTCnf.mappedFilesLocation);

			System.out.println("Block Number To Be Mapped: " + blockNumber);
			System.out
					.println("Block Data is-----\n" + blockData + "\n-------");

			synchronized (mapRunningMapperIdToTaskInfo) {
				mapRunningMapperIdToTaskInfo.get(taskId).taskCompleted = true;
				mapRunningMapperIdToTaskInfo.get(taskId).mapOutputFile = "job_"
						+ mapRunningMapperIdToTaskInfo.get(taskId).jobId
						+ "_map_" + taskId;
			}
			synchronized (tTCnf) {
				tTCnf.freeMapSlots++;
			}

			System.out.println("*********Map Task " + taskId
					+ " Done**************");
		}

		private String startMapping(String mapName, String inputContent) {
			String output = null;
			File jarDir = new File(tTCnf.jarFilesPath);
			String className = mapName;

			for (File jarFile : jarDir.listFiles()) {
				try {
					URL fileURL = jarFile.toURI().toURL();
					String jarURL = "jar:" + fileURL + "!/";
					URL urls[] = { new URL(jarURL) };
					URLClassLoader ucl = new URLClassLoader(urls);
					Class cls = Class.forName(className, true, ucl);
					Object obj = cls.newInstance();
					Class params[] = new Class[1];
					params[0] = String.class;
					Method m = cls.getDeclaredMethod("map", params);
					output = (String) m.invoke(obj, inputContent);
					break;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			return output;
		}

	}

	private class ReduceTask implements Runnable {

		int taskId;

		public ReduceTask(int taskId) {
			this.taskId = taskId;
		}

		@Override
		public void run() {
			performReduceTask();
		}

		private void performReduceTask() {
			System.out.println("*********Reduce Task " + taskId
					+ " Started************");
			HDFSMethods hdfsMethods = new HDFSMethods(tTCnf);

			Integer jobId = mapRunningReducerIdToTaskInfo.get(taskId).jobId;
			ArrayList<String> mapOutputFilesToBeReduced = mapRunningReducerIdToTaskInfo
					.get(taskId).mapOutputFilesToBeReduced;
			String outputFileName = mapRunningReducerIdToTaskInfo.get(taskId).outputFile;
			String reducerName = mapRunningReducerIdToTaskInfo.get(taskId).reducerName;

			String reducerOutputFileName = outputFileName + "_" + jobId + "_"
					+ taskId;

			try {
				BufferedWriter bufferedWriter = new BufferedWriter(
						new FileWriter(tTCnf.reducedFilesLocation + "\\"
								+ reducerOutputFileName, true));
				for (String fileToBeReduced : mapOutputFilesToBeReduced) {
					String fileContent = hdfsMethods
							.readRemoteFile(fileToBeReduced);
					String reducedOutputContent = startReduceTask(reducerName,
							fileContent);
					bufferedWriter.write(reducedOutputContent + "\n");
				}
				bufferedWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			// write file in hdfs
			hdfsMethods.writeRemoteFile(reducerOutputFileName,
					tTCnf.reducedFilesLocation);

			System.out.println("Reducer Output File Name: " + outputFileName);

			synchronized (mapRunningReducerIdToTaskInfo) {
				mapRunningReducerIdToTaskInfo.get(taskId).taskCompleted = true;
			}
			synchronized (tTCnf) {
				tTCnf.freeReduceSlots++;
			}

			System.out.println("*********Reduce Task " + taskId
					+ " Done**************");
		}

		private String startReduceTask(String reducerName, String inputContent) {
			String outputFileContent = null;
			File jarDir = new File(tTCnf.jarFilesPath);
			String className = reducerName;

			for (File jarFile : jarDir.listFiles()) {
				try {
					URL fileURL = jarFile.toURI().toURL();
					String jarURL = "jar:" + fileURL + "!/";
					URL urls[] = { new URL(jarURL) };
					URLClassLoader ucl = new URLClassLoader(urls);
					Class cls = Class.forName(className, true, ucl);
					Object obj = cls.newInstance();
					Class params[] = new Class[1];
					params[0] = String.class;
					Method m = cls.getDeclaredMethod("reduce", params);
					outputFileContent = (String) m.invoke(obj, inputContent);

					System.out.println("ClassName: " + className
							+ " Found In jar File: " + jarFile.getName());
					break;
				} catch (Exception e) {
					System.out.println("Still Serching for ClassName: "
							+ className + " in jar Files");
					// e.printStackTrace();
				}
			}
			return outputFileContent;
		}
	}

}