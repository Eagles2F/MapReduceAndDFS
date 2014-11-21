package mapreduce.master;
/*
 * This class represents a MapReduce job which is going to be running on the facility
 */


import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

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
