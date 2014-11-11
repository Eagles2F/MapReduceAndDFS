package mapreduce;

import java.io.Serializable;

import mapreduce.TaskStatus.taskPhase;



public class TaskStatus implements Serializable{
  //enumeration for reporting current phase of a task. 
    public static enum taskPhase{STARTING, MAP, SHUFFLE, SORT, REDUCE, CLEANUP}

    // what state is the task in?
    public static enum taskState{QUEUING,RUNNING, SUCCEEDED, FAILED, UNASSIGNED, KILLED,COMMIT_PENDING, FAILED_UNCLEAN, KILLED_UNCLEAN,COMPLETE}
    private final int taskId;
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
}