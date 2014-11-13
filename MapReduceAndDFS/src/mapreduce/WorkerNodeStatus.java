package mapreduce;

import java.io.Serializable;
import java.util.HashMap;

public class WorkerNodeStatus implements Serializable{
    private String trackerName;
    private int workerId;
    private int masterAdd;
    private int masterPort;
    private boolean alive;
    private HashMap<Integer,TaskStatus> taskReports;
    private int failures;
    private int maxTasks;

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
	public boolean isAlive() {
		return alive;
	}
	public void setAlive(boolean alive) {
		this.alive = alive;
	}
}