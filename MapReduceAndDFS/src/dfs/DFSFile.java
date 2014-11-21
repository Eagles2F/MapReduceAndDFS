package dfs;

import java.io.Serializable;

public class DFSFile extends DFSFileEntry implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1057718551556472988L;
	private String nodeAddress = null;
	private String dupNodeAddress = null;
	private int nodeId;
	private int dupId;
	
	private String nodeLocalFilePath;
	private String dupLocalFilePath;
	
	public DFSFile(String newName){
		this.name = newName;
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

	public String getNodeAddress() {
		return nodeAddress;
	}

	public void setNodeAddress(String nodeAddress) {
		this.nodeAddress = nodeAddress;
	}

	public String getDupNodeAddress() {
		return dupNodeAddress;
	}

	public void setDupNodeAddress(String dupNodeAddress) {
		this.dupNodeAddress = dupNodeAddress;
	}

	public String getNodeLocalFilePath() {
		return nodeLocalFilePath;
	}

	public void setNodeLocalFilePath(String nodeLocalFilePath) {
		this.nodeLocalFilePath = nodeLocalFilePath;
	}

	public String getDupLocalFilePath() {
		return dupLocalFilePath;
	}

	public void setDupLocalFilePath(String dupLocalFilePath) {
		this.dupLocalFilePath = dupLocalFilePath;
	}

	public int getNodeId() {
		return nodeId;
	}

	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}

	public int getDupId() {
		return dupId;
	}

	public void setDupId(int dupId) {
		this.dupId = dupId;
	}

}
