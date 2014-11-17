package utility;

import java.io.Serializable;
import mapreduce.Task;
import mapreduce.WorkerNodeStatus;

/**
* Message class used to send between manager and worker
 * @param <DownloadType>
* @Author Yifan Li
* @Author Jian Wang
*/
public class DFSMessage implements Serializable {
    /**
     * 
     */
    

    /**
     * 
     */
    
    public enum msgType {
        COMMAND,
        RESPONSE,

    }
    public enum msgResult{
        SUCCESS,
        FAILURE
    }
    
    public enum rspId{
        CREATERSP,
        REMOVERSP,
        GETFILESRSP,
        DOWNLOADRSP
    }
    
    public enum nodeType{
        NAMENODE,
        DATANODE,
    }
    
    public enum DownloadType{
        TXT,
        OBJECT
    }
    private msgType messageType;
    private int recordLenth;
    private int startIndex;
    private int chunkLenth;
    private String cause;
    private int workerID;
    
    private String targetFileName;
    private String targetPath;
    private String targetNodeAddr;
    private int    targetPortNum;
    
    private DownloadType downloadType;
    private rspId responseId;
    
    private nodeType messageSource;
    private msgResult result;
    private DFSCommandId cmdId;
    private String localPath;
    private String localFileName;
    
    public String getLocalPath() {
        return localPath;
    }
    public void setLocalPath(String localPath) {
        this.localPath = localPath;
    }
    public String getLocalFileName() {
        return localFileName;
    }
    public void setLocalFileName(String localFileName) {
        this.localFileName = localFileName;
    }
    public rspId getResponseId() {
        return responseId;
    }
    public void setResponseId(rspId responseId) {
        this.responseId = responseId;
    }
   public msgResult getResult() {
        return result;
    }
    public void setResult(msgResult result) {
        this.result = result;
    }
    public String getCause() {
        return cause;
    }
    public void setCause(String cause) {
        this.cause = cause;
    }
    public void setDownloadType(DownloadType downloadType) {
        this.downloadType = downloadType;
    }
    public String getTargetNodeAddr() {
        return targetNodeAddr;
    }
    public void setTargetNodeAddr(String targetNodeAddr) {
        this.targetNodeAddr = targetNodeAddr;
    }
    public int getTargetPortNum() {
        return targetPortNum;
    }
    public void setTargetPortNum(int targetPortNum) {
        this.targetPortNum = targetPortNum;
    }
    public DownloadType getDownloadType() {
        return downloadType;
    }
    
    public int getRecordLenth() {
        return recordLenth;
    }
    public void setRecordLenth(int recordLenth) {
        this.recordLenth = recordLenth;
    }
    public int getStartIndex() {
        return startIndex;
    }
    public void setStartIndex(int startIndex) {
        this.startIndex = startIndex;
    }
    public int getChunkLenth() {
        return chunkLenth;
    }
    public void setChunkLenth(int chunkLenth) {
        this.chunkLenth = chunkLenth;
    }
    public msgType getMessageType() {
        return messageType;
    }
    public void setMessageType(msgType messageType) {
        this.messageType = messageType;
    }
    public DFSCommandId getCmdId() {
        return cmdId;
    }
    public void setCmdId(DFSCommandId cmdId) {
        this.cmdId = cmdId;
    }
	public String getTargetFileName() {
		return targetFileName;
	}
	public void setTargetFileName(String targetFileName) {
		this.targetFileName = targetFileName;
	}
	public String getTargetPath() {
		return targetPath;
	}
	public void setTargetPath(String targetPath) {
		this.targetPath = targetPath;
	}
	public nodeType getMessageSource() {
		return messageSource;
	}
	public void setMessageSource(nodeType messageSource) {
		this.messageSource = messageSource;
	}
	public int getWorkerID() {
		return workerID;
	}
	public void setWorkerID(int workerID) {
		this.workerID = workerID;
	}

       
}