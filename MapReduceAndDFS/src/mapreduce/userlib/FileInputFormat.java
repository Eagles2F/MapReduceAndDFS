package mapreduce.userlib;

import java.io.Serializable;

/*
 * This is the user API for specifying input files, assuming the input file is a file with a lot of data inside.
 * And we also assume that the record structure is just one line.
 *  
 */
public class FileInputFormat implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4063078446810176149L;
	private String path;   // DFS directory for the input file which are chopped off into different pieces
	private int size_file;
	
	public FileInputFormat(String path, int size){
		this.path = path;
		this.setSize_file(size);
		
	}

	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}

	public int getSize_file() {
		return size_file;
	}

	public void setSize_file(int size_file) {
		this.size_file = size_file;
	}
	
	
}
