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
    private int noReportCnt;//this count will increment in the monitor task, and will be reset by the statusReport
                            // if this count is large than 2, that means there has been more than 15 seconds not status report
    private HashMap<Integer,TaskStatus> taskReports;
    private int maxTasks;
    private boolean dataNode_alive;
    
    public WorkerNodeStatus(int Id){
        workerId = Id;
        taskReports = new HashMap<Integer,TaskStatus>();
        setAlive(true);
        maxTasks = 0;
    }
    public WorkerNodeStatus(){
        taskReports = new HashMap<Integer,TaskStatus>();
        setAlive(true);
        maxTasks = 0;
    }
    
    public void setTaskReports(HashMap<Integer, TaskStatus> taskReports) {
        this.taskReports = taskReports;
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
	public boolean isDataNode_alive() {
		return dataNode_alive;
	}
	public void setDataNode_alive(boolean dataNode_alive) {
		this.dataNode_alive = dataNode_alive;
	}
    public void resetNoReportCnt() {
        noReportCnt = 0;
    }
    public void incrementNoReportCnt() {
        this.noReportCnt++;
    }
    public int getNoReportCnt() {
        
        return noReportCnt;
    }
	
    
}