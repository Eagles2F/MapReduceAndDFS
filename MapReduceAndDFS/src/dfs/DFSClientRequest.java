package dfs;

import java.io.Serializable;

public class DFSClientRequest implements Serializable {
	private static final long serialVersionUID = -1127631812407270989L;
	
	public enum RequestType{
		InputUpload
	}
	private RequestType reqType;
	private String JobName; // the identifier to connect an inputfile to a job
	private String InputFilePath;//the local path where the file is
	private String DesDFSDir; // the destination abstract DFSDirectory
	private String FileName;
	private int fileLineNum;
	private int downloadServerPort;
	public DFSClientRequest(RequestType rt){
		this.setReqType(rt);
	}

	public RequestType getReqType() {
		return reqType;
	}

	public void setReqType(RequestType reqType) {
		this.reqType = reqType;
	}

	public String getJobName() {
		return JobName;
	}

	public void setJobName(String jobName) {
		JobName = jobName;
	}

	public String getInputFilePath() {
		return InputFilePath;
	}

	public void setInputFilePath(String inputFilePath) {
		InputFilePath = inputFilePath;
	}

	public String getDesDFSDir() {
		return DesDFSDir;
	}

	public void setDesDFSDir(String desDFSDir) {
		DesDFSDir = desDFSDir;
	}

	public int getFileLineNum() {
		return fileLineNum;
	}

	public void setFileLineNum(int fileLineNum) {
		this.fileLineNum = fileLineNum;
	}

	public String getFileName() {
		return FileName;
	}

	public void setFileName(String fileName) {
		FileName = fileName;
	}

	public int getDownloadServerPort() {
		return downloadServerPort;
	}

	public void setDownloadServerPort(int downloadServerPort) {
		this.downloadServerPort = downloadServerPort;
	}
	
}
