package mapreduce.userlib;

import java.io.Serializable;

public class FileOutputFormat implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7621727429692886426L;
	private String path;

	public FileOutputFormat(String path){
		this.path = path;
	}
	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}
	
}
