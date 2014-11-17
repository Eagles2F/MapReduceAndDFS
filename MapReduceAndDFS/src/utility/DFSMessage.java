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
    public msgType getMessageType() {
        return messageType;
    }
    public void setMessageType(msgType messageType) {
        this.messageType = messageType;
    }
    private DFSCommandId cmdId;

    public DFSCommandId getCmdId() {
        return cmdId;
    }
    public void setCmdId(DFSCommandId cmdId) {
        this.cmdId = cmdId;
    }
    
    private rspId responseId;
    private nodeType sourceNode;
    public nodeType getSourceNode() {
        return sourceNode;
    }
    public void setSourceNode(nodeType sourceNode) {
        this.sourceNode = sourceNode;
    }
    private nodeType targetNode;
    public nodeType getTargetNode() {
        return targetNode;
    }
    public void setTargetNode(nodeType targetNode) {
        this.targetNode = targetNode;
    }
    
    public rspId getResponseId() {
        return responseId;
    }
    public void setResponseId(rspId responseId) {
        this.responseId = responseId;
    }
    private msgResult result;
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
    private String cause;
    private int workerID;
    private String fileName;
    private DownloadType downloadType;
    public void setDownloadType(DownloadType downloadType) {
        this.downloadType = downloadType;
    }
    public String getFileName() {
        return fileName;
    }
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
    private int chunkNum;
    public int getChunkNum() {
        return chunkNum;
    }
    public void setChunkNum(int chunkNum) {
        this.chunkNum = chunkNum;
    }

    private String targetNodeAddr;
    private int    targetPortNum;
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
        // TODO Auto-generated method stub
        return downloadType;
    }
       
}