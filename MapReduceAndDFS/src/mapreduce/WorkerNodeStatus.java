package mapreduce;

import java.io.Serializable;
import java.util.HashMap;

public class WorkerNodeStatus implements Serializable{
    /**
     * 
     */
    private static final long serialVersionUID = -4950825013774155452L;
    private int workerId;
    private boolean alive;
    private HashMap<Integer,TaskStatus> taskReports;
    private int maxTasks;
    private int taskType;
    

    public WorkerNodeStatus(int Id){
        workerId = Id;
        taskReports = new HashMap<Integer,TaskStatus>();
        alive = true;
        maxTasks = 0;
    }
    public WorkerNodeStatus(){
        taskReports = new HashMap<Integer,TaskStatus>();
        alive = true;
        maxTasks = 0;
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
	
    public void setTaskType(int type){
        taskType = type;
    }
    public int getTaskType(){
        return taskType;
    }
}