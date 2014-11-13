package mapreduce;

import java.io.Serializable;



public class TaskStatus implements Serializable{
  /**
     * 
     */
    private static final long serialVersionUID = 2855645783111865212L;

    //enumeration for reporting current phase of a task. 
    public static enum taskPhase{STARTING, MAP, SHUFFLE, SORT, REDUCE, CLEANUP}
    
    // what state is the task in?
    public static enum taskState{UNSENT,SENT,RECEIVED,QUEUING,RUNNING, SUCCEEDED, FAILED, KILLED,COMPLETE}
    private final int taskId;
    private int jobId;
    private float progress;
    private volatile taskState runState;
    private volatile taskPhase currentPhase;
    private String diagnosticInfo;
    private String stateString;
    private String taskTracker;
    
      
    private long startTime; 
    private long finishTime; 
    private long outputSize;
    public TaskStatus(int taskid){
        taskId = taskid;
        runState = taskState.UNSENT;
    }
    
    public void setState(taskState state){
        runState = state;
    }
    public taskState getState(){
        return runState;
    }
    
    public void setPhase(taskPhase phase){
        currentPhase = phase;
    }
    
    public void setProgress(float p){
        progress = p;
    }
    
    public float getProgress(){
        return progress;
    }

    public taskPhase getPhase() {
        // TODO Auto-generated method stub
        return currentPhase;
    }

	public int getJobId() {
		return jobId;
	}

	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

	public int getTaskId() {
		return taskId;
	}
}