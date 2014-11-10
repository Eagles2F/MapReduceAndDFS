package mapreduce;

import java.util.HashMap;

public class WorkerNodeStatus{
    private String trackerName;
    private int workerId;
    private int masterAdd;
    private int masterPort;

    private HashMap<Integer,TaskStatus> taskReports;
    private int failures;
    private int maxTasks;
    
    public WorkerNodeStatus(int Id){
        workerId = Id;
        taskReports = new HashMap<Integer,TaskStatus>();
    }
    public WorkerNodeStatus(){
        taskReports = new HashMap<Integer,TaskStatus>();
    }
    
    public void setMaxTask(int n){
        maxTasks = n;
    }
    public int getMaxTask(){
        return maxTasks;
    }
    
    public void setWorkerId(int i){
        workerId = i;
    }
    public int getWorkerId(){
        return workerId;
    }
    
    public HashMap<Integer,TaskStatus> getTaskReports(){
        return taskReports;
        
    }
}