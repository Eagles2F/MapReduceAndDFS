package mapreduce.master;
/*
 * This class represents a MapReduce job which is going to be running on the facility
 */


import java.net.Socket;
import java.util.ArrayList;

import mapreduce.Task;
import mapreduce.TaskStatus;
import mapreduce.fileIO.SplitFile;
import mapreduce.fileIO.UserInputFiles;
import mapreduce.userlib.Job;

public class MapReduceJob {
	private int jobId;
	private Socket clientSocket;
	private Job job;
	private ArrayList<Task> MapTasks;
	private ArrayList<TaskStatus> MapTaskStatus;
	private ArrayList<SplitFile> SplitList;
	public MapReduceJob(Socket soc,Job job,int jobid){
		this.clientSocket =soc;
		this.job = job;
		this.jobId = jobid;
	}
	
	/*
	 * This method serves to split the job into MapTasks.
	 */
	public void Split(){
		//get the inputfiles which offers the access to the user input data
		UserInputFiles uif = new UserInputFiles(job.getFif());
		
		//compute the block_size for each split file
		int block_size = 0;
		/* Scheduling 1:
			  a.get the number of available MapTask computing ability  --  num
			  b.set the block_size as num_record / num
		*/
		/* Scheduling 2:
		 * 	  just set the block_size as some magic number 
		 */
		/* Scheduling 3:
		 * 	  a.sum the max_mapper_num on each worker node as sum
		 *    b.set the block_size as num_record/ sum
		 */
		
		//do the actual splitting
		int size_file=job.getFif().getSize_file();
		for(int i=0;i<(size_file-block_size);i = i+block_size){
			SplitFile sf = new SplitFile(i,block_size,uif);
			SplitList.add(sf);
		}
		//split the final file differently
		if(size_file%block_size != 0){
			SplitFile sf = new SplitFile(size_file-(size_file%block_size),size_file%block_size,uif);
			SplitList.add(sf);
		}		
	}
	
	//generate the Maptasks
	public void MapTaskGen(){
		for(SplitFile sf:SplitList){
			Task task = new Task();
			task.setJobId(this.jobId);
			task.setType(0);
			task.setSplit(sf);
			task.setTaskId(this.MapTasks.size());
			task.setMapClass(this.job.getMapperClass());
			task.setMapInputKeyClass(this.job.getMapperClass().getTypeParameters()[0].getClass());
			task.setMapInputValueClass(this.job.getMapperClass().getTypeParameters()[1].getClass());
			task.setMapOutputKeyClass(this.job.getMapperClass().getTypeParameters()[2].getClass());
			task.setMapOutputValueClass(this.job.getMapperClass().getTypeParameters()[3].getClass());
			this.MapTasks.add(task);
			TaskStatus taskStatus = new TaskStatus(task.getTaskId());
			this.MapTaskStatus.add(taskStatus);
		}
	} 
	
	public ArrayList<TaskStatus> getMapTaskStatus() {
		return MapTaskStatus;
	}

	public void setMapTaskStatus(ArrayList<TaskStatus> mapTaskStatus) {
		MapTaskStatus = mapTaskStatus;
	}

	public Socket getClientSocket() {
		return clientSocket;
	}

	public void setClientSocket(Socket clientSocket) {
		this.clientSocket = clientSocket;
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

}
