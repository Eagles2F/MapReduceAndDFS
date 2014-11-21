package mapreduce.master;
/*
 * This class represents a MapReduce job which is going to be running on the facility
 */


import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import utility.DFSMessage;
import mapreduce.Task;
import mapreduce.TaskStatus;
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
	public MapReduceJob(ObjectOutputStream oos,Job job,int jobid){
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
	}
	
	
	//generate the Maptasks
	public void MapTaskGen(){
		for(SplitFile sf:SplitList){
			Task task = new Task();
			task.setJobId(this.jobId);
			task.setType(0);
			task.setSplit(sf);
			task.setReducerNum(job.getReducerNum());
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
