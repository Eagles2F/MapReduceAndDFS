package mapreduce.master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

import dfs.DFSFile;
import dfs.DFSInputFile;
import dfs.Range;
import utility.CommandType;
import utility.Message;
import utility.Message.msgType;
import mapreduce.Task;
import mapreduce.TaskStatus.taskState;
import mapreduce.WorkerNodeStatus;
import mapreduce.fileIO.SplitFile;
import mapreduce.fileIO.UserInputFiles;
import mapreduce.userlib.Job;

/*
 * This server is responsible to listen to the job submit request from the MapReduce client and add the Job to
 * the jobMap in master. The job will be split into MapTask and send to the workers.   
 *@Author: Yifan Li
 *@Author: Jian Wang
 *
 *@Date: 11/9/2014
 *@Version:0.00,developing version
 */
public class JobReceiveServer implements Runnable{

	private Master master;
	
	//the port this server is listening to 
	private int port; 
	
	//count the number of the jobs requested
	private int jobCnt;
	
	private volatile boolean running;
	ServerSocket serverSocket;
	
	public JobReceiveServer(int port, Master master){
		this.port = port;
		this.master = master;
		running =true;
		jobCnt = 0;
		try {
		      serverSocket = new ServerSocket(port);
		      } catch (IOException e) {
		        e.printStackTrace();
		        System.out.println("failed to create the socket server");
		        System.exit(0);
		}
		System.out.println("create JobReceiverServer");
	}
	
	//send the mapTask method
	private void sendMapTasks(MapReduceJob job) throws IOException{
		//Send the task to the node where the split files are located
			for(int i=0;i<job.getMapTasks().size();i++){
				Task t=job.getMapTasks().get(i);
				
				//get the nodeId where files are located
				int nodeId =t.getSplit().getUserInputFiles().fileChunk.getNodeId();
				
				Message msg = new Message();
				msg.setMessageType(msgType.COMMAND);
				msg.setCommandId(CommandType.START);
				msg.setJobId(job.getJobId());
				msg.setTaskId(t.getTaskId());
				msg.setTaskItem(t);
				msg.setWorkerID(nodeId);
				master.workerOosMap.get(nodeId).writeObject(msg);
				job.getMapTaskStatus().get(i).setState(taskState.SENT);		
				System.out.println("MapTask to worker "+nodeId+" has been sent!");
			}
	}
	
	/*
	 * This method serves to split the job into MapTasks.
	 */	
	public void Split(MapReduceJob job) throws IOException{
		/*
		 * 	  split the input file based on each file chunk and MaxTask on each node
		 */
					//get the DFSInputFile 
					DFSInputFile userInput = (DFSInputFile)master.getNameNodeServer().getRootDir().getEntry(job.getJob().getFif().getPath());
					
					//get the file chunks
					HashMap<Range,DFSFile> fileChunks = userInput.getFileChunks();
					
					//split each chunk and add to the split list
					for(Range r:fileChunks.keySet()){
						int size = r.endId - r.startId; //size of the chunk
						
						//get the worker status where this chunk is located
						WorkerNodeStatus nodeStatus = master.workerStatusMap.get(fileChunks.get(r).getNodeId());
						
						//split the chunk to the number of max task numbers which a node can support 
						int splitNum = nodeStatus.getMaxTask();
						
						//setup the filechunk
						UserInputFiles uif = new UserInputFiles(fileChunks.get(r),r.startId,size);
					
						//do the splitting, assuming size is far bigger than splitNum
						int start =r.startId;
						if(size%splitNum == 0){ 
							for(int i=0;i<splitNum;i++){
								SplitFile sf = new SplitFile(start,size/splitNum,uif);
								start = start+size/splitNum;
								job.getSplitList().add(sf);
								System.out.println("SSSSSSSSSSSSSSSSSSS"+sf.getUserInputFiles().fileChunk.getName());
							}
						}else{
							for(int i=0;i<splitNum-1;i++){
								SplitFile sf = new SplitFile(start,size/splitNum,uif);
								start = start+size/splitNum;
								job.getSplitList().add(sf);
							}
							SplitFile sf = new SplitFile(start,size/splitNum+size%splitNum,uif);
							job.getSplitList().add(sf);
							
						}
					}
						
				System.out.println("number of splits:"+job.getSplitList().size());	
	}
	
	
	@Override
	public void run() {
		   try{
	    	   System.out.println("JobReceiveServer is waiting for new MapReduceJobs on the port: "+this.port);
	           while(running){
	               Socket jobSocket = serverSocket.accept();
	               System.out.println("MapReduceJob: "+jobSocket.getInetAddress()+":"+jobSocket.getPort()+" join in");
	    
	               //Parse the received Job submission and Create the job instance on the master
	               ObjectOutputStream oos = new ObjectOutputStream(jobSocket.getOutputStream());
	   			   ObjectInputStream ois = new ObjectInputStream(jobSocket.getInputStream());
	   			   
	   			   //read the job from the input stream
				   Job received_job = (Job)ois.readObject();
	   			   
	               MapReduceJob job = new MapReduceJob(oos,received_job,jobCnt);
	               
	               //Split the job
	               Split(job);
	               System.out.println("Split finished!");
	               //Create the MapTasks
	               job.MapTaskGen();
	               System.out.println("Task generation finished!");
	               //send the MapTasks away
	               sendMapTasks(job);
	          
	               //store the Mapreduce job into the ConcurrentHashmap
	               master.jobMap.put(jobCnt, job);
	               jobCnt++;
	           } 
	       }catch(IOException e){
	           e.printStackTrace();
	           System.out.println("socket server accept failed");
	       } catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	       try {
	        serverSocket.close();
	       } catch (IOException e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
	        System.out.println("socket Server failed to close");
	    }
	        
		
	}
	
	public void stop(){
		running = false;
	}
}
