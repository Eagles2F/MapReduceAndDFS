package mapreduce.master;
/*
 * This class represents a MapReduce job which is going to be running on the facility
 */


import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import dfs.DFSFile;
import dfs.DFSInputFile;
import dfs.Range;
import utility.ClientMessage;
import utility.CommandType;
import utility.DFSMessage;
import utility.Message;
import utility.Message.msgType;
import mapreduce.Task;
import mapreduce.TaskStatus;
import mapreduce.TaskStatus.taskState;
import mapreduce.fileIO.SplitFile;
import mapreduce.userlib.Job;

public class MapReduceJob {
	private int jobId;
	private ObjectOutputStream clientOOS;
	private Job job;
	private ArrayList<Task> MapTasks;
	private ArrayList<TaskStatus> MapTaskStatus;
	private ArrayList<Task> ReduceTasks;
	private ArrayList<TaskStatus> ReduceTaskStatus;
	private ArrayList<SplitFile> SplitList;
    private String mapperOutputPath;
    private ConcurrentHashMap<Integer,DFSMessage> dfsMsgConcurrentHashMap;
    private Master master;
    
	public MapReduceJob(ObjectOutputStream oos,Job job,int jobid,Master master){
		this.setClientOOS(oos);
		this.job = job;
		this.jobId = jobid;
		this.SplitList = new ArrayList<SplitFile>();
		this.MapTasks = new ArrayList<Task>();
		this.ReduceTasks = new ArrayList<Task>();
		this.ReduceTaskStatus = new ArrayList<TaskStatus>();
		this.MapTaskStatus = new ArrayList<TaskStatus>();
		this.mapperOutputPath = "../DFS/temp";
		this.dfsMsgConcurrentHashMap = new ConcurrentHashMap<Integer,DFSMessage>();
		this.master = master;
	}
	
	
	//generate the Maptasks
	public void MapTaskGen(){
		for(SplitFile sf:SplitList){
			Task task = new Task();
			task.setJobId(this.jobId);
			task.setType(0);
			task.setSplit(sf);
			task.setReducerNum(job.getReducerNum());
			System.out.println("ReducerNum:"+job.getReducerNum());
			task.setTaskId(this.MapTasks.size());
			task.setMapClass(this.job.getMapperClass());
			task.setReduceClass(this.job.getReducerClass());
			
			task.setOutputPath(mapperOutputPath);
			this.MapTasks.add(task);
			TaskStatus taskStatus = new TaskStatus(task.getTaskId());
			taskStatus.setTaskType(Task.MAP);
			this.MapTaskStatus.add(taskStatus);
		}
		System.out.println("Number of MapTasks:"+this.MapTasks.size());
	} 
	
	//Node failure lead to all the tasks running on that node to be FAILED
	public void NodeFail(int nodeId){
		for(TaskStatus ts:this.MapTaskStatus){
			if(ts.getWorkerId() == nodeId){
				ts.setState(taskState.FAILED);
			}
		}
		for(TaskStatus ts:this.ReduceTaskStatus){
			if(ts.getWorkerId() == nodeId){
				ts.setState(taskState.FAILED);
			}
		}
	}
	
	//recover each map task that is running on this node
	public void NodeFailMapTaskRecover(int nodeId){  
		System.out.println("Map Task failed due to node failure, started to recover the map tasks!!");
    		for(int i=0;i<this.getMapTasks().size();i++){
    			Task mapTask = this.getMapTasks().get(i);
    			if(mapTask.getWorkerId() == nodeId){ // if node failed, resent all the maptasks
    				
    				//update the splitFile in each task since data node has also failed.
    				//get the DFSInputFile 
					DFSInputFile userInput = (DFSInputFile)master.getNameNodeServer().getRootDir().getEntry(job.getFif().getPath());
					
					//get the file chunks
					HashMap<Range,DFSFile> fileChunks = userInput.getFileChunks();

					//retrieve the new file chunk by range
					Range r = new Range(mapTask.getSplit().getStartId(),mapTask.getSplit().getStartId()+mapTask.getSplit().getLength());
					DFSFile newFileChunk = fileChunks.get(r);
	                
					//update the file chunk
    				mapTask.getSplit().getUserInputFiles().fileChunk = newFileChunk;
    				
    				//get the nodeId where files are located
    				int newNodeId =mapTask.getSplit().getUserInputFiles().fileChunk.getNodeId();
    				
    				//send the recovery message to the newNodeId
    				Message msg = new Message();
    				msg.setMessageType(msgType.COMMAND);
    				msg.setCommandId(CommandType.START);
    				msg.setJobId(this.jobId);
    				msg.setTaskId(mapTask.getTaskId());
    				msg.setTaskItem(mapTask);
    				msg.setWorkerID(newNodeId);
    				
    				this.getMapTaskStatus().get(i).setWorkerId(newNodeId);
    				
    				try {
						master.workerOosMap.get(newNodeId).writeObject(msg);
					} catch (IOException e) {
						e.printStackTrace();
					}
    				this.getMapTaskStatus().get(i).setState(taskState.SENT);		
    				System.out.println("MapTask: "+mapTask.getTaskId()+" for Job: "+this.getJobId()+" has been recovered on Node:"+newNodeId);
               	}
    		}
	}
	
	//recover each reduce task that is running on this node
	public void NodeFailReduceTaskRecover(int nodeId){
        for(int k=0;k<this.ReduceTasks.size();k++){
           if(this.getReduceTaskStatus().get(k).getState() == taskState.FAILED){
        	   System.out.println("Reduce Task failed due to node failure, pls resubmit your job!!");
        	   this.KillTasks();
        	   master.jobMap.remove(this.jobId);
        	   try {
				this.clientOOS.writeObject(new ClientMessage(-1)); //job failed
			} catch (IOException e){
				e.printStackTrace();
			}//succeed!
        	   return;
           }
        }
	}
	
	
	//Task Status report
	public void TaskReport(){
		for(TaskStatus ts:this.MapTaskStatus){
			
			System.out.println("MapTask Id: "+ts.getTaskId()+" Task status: "+ts.getState());
		
		}
		for(TaskStatus ts:this.ReduceTaskStatus){
		
			System.out.println("ReduceTask Id: "+ts.getTaskId()+" Task status: "+ts.getState());
			
		}
	}
	
	//kill all the tasks which are running on the workers
	public void KillTasks(){
		for(TaskStatus ts:this.MapTaskStatus){
			if(ts.getState() == taskState.RECEIVED || 
					ts.getState() == taskState.QUEUING ||
							ts.getState() == taskState.RUNNING){
				//all these tasks inside these three states should be killed
				Message msg= new Message(msgType.COMMAND);
				msg.setCmdId(CommandType.KILLTASK);
				msg.setTaskItem(this.MapTasks.get(ts.getTaskId()));// reduce and map???????????????
				
				try {
					master.workerMangerServerMap.get(ts.getWorkerId()).sendToWorker(msg);
					System.out.println(" Kill Task Id: "+ts.getTaskId()+" On worker: "+ts.getWorkerId());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
					
		}
		for(TaskStatus ts:this.ReduceTaskStatus){
			if(ts.getState() == taskState.RECEIVED || 
					ts.getState() == taskState.QUEUING ||
							ts.getState() == taskState.RUNNING){
				//all these tasks inside these three states should be killed
				Message msg= new Message(msgType.COMMAND);
				msg.setCmdId(CommandType.KILLTASK);
				msg.setTaskItem(this.MapTasks.get(ts.getTaskId()));// reduce and map???????????????
				
				try {
					master.workerMangerServerMap.get(ts.getWorkerId()).sendToWorker(msg);
					System.out.println(" Kill Task Id: "+ts.getTaskId()+" On worker: "+ts.getWorkerId());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}			
		}
	}
	
	public ArrayList<TaskStatus> getMapTaskStatus() {
		return MapTaskStatus;
	}

	public void setMapTaskStatus(ArrayList<TaskStatus> mapTaskStatus) {
		MapTaskStatus = mapTaskStatus;
	}

	public Job getJob() {
		return job;
	}

	public void setJob(Job job) {
		this.job = job;
	}

	public int getJobId() {
		return jobId;
	}

	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

	public ArrayList<Task> getMapTasks() {
		return MapTasks;
	}

	public void setMapTasks(ArrayList<Task> mapTasks) {
		MapTasks = mapTasks;
	}

	public ArrayList<SplitFile> getSplitList() {
		return SplitList;
	}

	public void setSplitList(ArrayList<SplitFile> splitList) {
		SplitList = splitList;
	}


	public ArrayList<Task> getReduceTasks() {
		return ReduceTasks;
	}


	public void setReduceTasks(ArrayList<Task> reduceTasks) {
		ReduceTasks = reduceTasks;
	}


	public ArrayList<TaskStatus> getReduceTaskStatus() {
		return ReduceTaskStatus;
	}


	public void setReduceTaskStatus(ArrayList<TaskStatus> reduceTaskStatus) {
		ReduceTaskStatus = reduceTaskStatus;
	}


	public ObjectOutputStream getClientOOS() {
		return clientOOS;
	}


	public void setClientOOS(ObjectOutputStream clientOOS) {
		this.clientOOS = clientOOS;
	}


    public ConcurrentHashMap<Integer,DFSMessage> getDfsMsgConcurrentHashMap() {
        return dfsMsgConcurrentHashMap;
        
    }
    
    

}
