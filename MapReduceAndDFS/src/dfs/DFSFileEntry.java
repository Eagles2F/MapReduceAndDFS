package dfs;

/*
 * This class is the DFS filesystem's file entry class, it will be extended by directories and files  
 */

public abstract class DFSFileEntry {
	public String name;
	public abstract String getName();
	public abstract boolean rename(String newName);
	public abstract String getType();
}
