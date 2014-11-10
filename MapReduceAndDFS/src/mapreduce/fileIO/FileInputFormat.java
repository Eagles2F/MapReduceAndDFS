package mapreduce.fileIO;
/*
 * This is the user API for specifying input files
 *  
 */
public class FileInputFormat {
	private String path;
	private int size_per_file;
	
	public FileInputFormat(String path, int size){
		this.path = path;
		this.size_per_file = size;
	}
	
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public int getSize_per_file() {
		return size_per_file;
	}
	public void setSize_per_file(int size_per_file) {
		this.size_per_file = size_per_file;
	}
	
}
