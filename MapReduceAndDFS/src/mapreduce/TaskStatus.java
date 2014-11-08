package mapreduce;



public class TaskStatus{
  //enumeration for reporting current phase of a task. 
    public static enum taskPhase{STARTING, MAP, SHUFFLE, SORT, REDUCE, CLEANUP}

    // what state is the task in?
    public static enum taskState{RUNNING, SUCCEEDED, FAILED, UNASSIGNED, KILLED,COMMIT_PENDING, FAILED_UNCLEAN, KILLED_UNCLEAN}
    private final int taskId;
    private float progress;
    private volatile taskState runState;
    private String diagnosticInfo;
    private String stateString;
    private String taskTracker;
      
    private long startTime; 
    private long finishTime; 
    private long outputSize;
    public TaskStatus(int taskid){
        taskId = taskid;
    }
}