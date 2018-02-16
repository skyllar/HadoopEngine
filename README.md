# HadoopEngine-Implementation

The project invloves implementation of the two key core components:
1. File System (Similar to HDFS)
2. Map-Reduce Framework

The project supports above functionality with following key services:

 A: Server Side 
  1. NameNode
  2. JobTracker
 
 B: Client Side
  1. DataNode
  2. TaskTracker

Some Background:

DataNode will keep on sending "HeartBeat" to NameNode informing "am still ALIVE" as well as sends "BlockReport" giving information of all blocks present. Similar type of communication will happen between JobTracker and TaskTracker informing about: "Aliveness" and "Task Schedule/Completion reports".

Aided Fearure:

The project also achieves the fault tolerance through replication of file blocks over multiple DataNodes (i.e. multiple PC's) with single point of failure to be JobTracker/NameNode services.
