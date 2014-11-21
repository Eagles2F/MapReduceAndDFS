package utility;
import java.io.Serializable;

import mapreduce.Task;
import mapreduce.WorkerNodeStatus;

/**
* Message class used to send between manager and worker
* @Author Yifan Li
* @Author Jian Wang
*/
public class Message implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7262370837710173507L;

	/**
	 * 
	 */
	
	public enum msgType {
        COMMAND,
        RESPONSE,
        INDICATION
    }
    public enum msgResult{
        SUCCESS,
        FAILURE
    }
    private msgType messageType;
    private CommandType cmdId;
    private ResponseType resId;
    private IndicationType indId;
    private int jobId;
    private int taskId;
    private Task taskItem;
    private int sourceNode;
    private int targetNode;
    private msgResult result;
    private String cause;
    private int workerID;
    private WorkerNodeStatus workerStatus;
    
    public Message(){
        workerStatus = new WorkerNodeStatus();
    }
    
    public WorkerNodeStatus getWorkerStatus(){
        return workerStatus;
    }
    
    
    public Message (msgType type){
        messageType = type;
        workerStatus = new WorkerNodeStatus();
        
    }
    
    
    
    /*get methods*/
    public msgType getMessageType(){
        return messageType;
    }
    
    public CommandType getCommandId(){
        return cmdId;
    }
    
    public ResponseType getResponseId(){
        return resId;
    }
    
    public int getJobId(){
        return jobId;
    }
    
    
    
    public Task getTask(){
        return taskItem;
    }
    
    
    
    public msgResult getResult(){
        return result;
    }
    
    public String getCause(){
        return cause;
    }
    
    
    /*set method*/
    
    public void setMessageType(msgType type){
        messageType = type ;
    }
    
    public void setCommandId(CommandType msgId){
        cmdId = msgId;
    }
    
    public void setResponseId(ResponseType msgId){
        resId = msgId;
    }
    
    public void setJobId(int job_id){
        jobId = job_id;
    }
    
    public void setTaskId(int task_id){
        taskId = task_id;
    }
    
    
    
    public void setResult(msgResult success){
        result = success;
    }
    
    public void setCause(String c){
        cause = c;
    }
    
	public int getWorkerID() {
		return workerID;
	}



	public void setWorkerID(int workerID) {
		this.workerID = workerID;
	}



    public int getTaskId() {
        
        return taskId;
    }

    public void setIndicationId(IndicationType id) {
        // TODO Auto-generated method stub
        indId = id;
    }
    
    public IndicationType getIndicationId() {
        // TODO Auto-generated method stub
        return indId;
    }

	public CommandType getCmdId() {
		return cmdId;
	}

	public void setCmdId(CommandType cmdId) {
		this.cmdId = cmdId;
	}

	public ResponseType getResId() {
		return resId;
	}

	public void setResId(ResponseType resId) {
		this.resId = resId;
	}

	public IndicationType getIndId() {
		return indId;
	}

	public void setIndId(IndicationType indId) {
		this.indId = indId;
	}

	public Task getTaskItem() {
		return taskItem;
	}

	public void setTaskItem(Task taskItem) {
		this.taskItem = taskItem;
	}

	public int getSourceNode() {
		return sourceNode;
	}

	public void setSourceNode(int sourceNode) {
		this.sourceNode = sourceNode;
	}

	public int getTargetNode() {
		return targetNode;
	}

	public void setTargetNode(int targetNode) {
		this.targetNode = targetNode;
	}

	public void setWorkerStatus(WorkerNodeStatus workerStatus) {
		this.workerStatus = workerStatus;
	}

    

    
}