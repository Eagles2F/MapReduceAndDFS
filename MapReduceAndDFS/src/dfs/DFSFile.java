package dfs;

public class DFSFile extends DFSFileEntry{
	
	private String nodeAddress = null;
	private String dupNodeAddress = null;
	private int portNum;
	private int dupPortNum;
	
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

	public int getPortNum() {
		return portNum;
	}

	public void setPortNum(int portNum) {
		this.portNum = portNum;
	}

	public int getDupPortNum() {
		return dupPortNum;
	}

	public void setDupPortNum(int dupPortNum) {
		this.dupPortNum = dupPortNum;
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

}
