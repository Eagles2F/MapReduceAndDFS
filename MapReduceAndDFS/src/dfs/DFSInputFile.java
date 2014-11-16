package dfs;

import java.util.HashMap;

public class DFSInputFile extends DFSFileEntry{

	private HashMap<Range,DFSFile> fileChunks = new HashMap<Range,DFSFile>(); // file chunks chopped off from the original big file.
	private int numOfRecords;
	
	public DFSInputFile(String newname){
		this.name = newname;
	}
	
	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public boolean rename(String newName) {
		this.name = newName;
		return true;
	}

	@Override
	public String getType() {
		
		return "File";
	}

	public int getNumOfRecords() {
		return numOfRecords;
	}

	public void setNumOfRecords(int numOfRecords) {
		this.numOfRecords = numOfRecords;
	}

	public HashMap<Range,DFSFile> getFileChunks() {
		return fileChunks;
	}

	public void setFileChunks(HashMap<Range,DFSFile> fileChunks) {
		this.fileChunks = fileChunks;
	}
}


