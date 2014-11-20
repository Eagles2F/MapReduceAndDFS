package dfs;

import java.util.concurrent.ConcurrentHashMap;

public class DFSJobStatus {
	private DFSInputFile input;
	private ConcurrentHashMap<Integer,Boolean> uploadStatusMap;
	
	public DFSJobStatus(DFSInputFile input){
		this.input = input;
	}
	
	public DFSInputFile getInput() {
		return input;
	}
	public void setInput(DFSInputFile input) {
		this.input = input;
	}
	public ConcurrentHashMap<Integer,Boolean> getUploadStatusMap() {
		return uploadStatusMap;
	}
	public void setUploadStatusMap(ConcurrentHashMap<Integer,Boolean> uploadStatusMap) {
		this.uploadStatusMap = uploadStatusMap;
	}
	
}
