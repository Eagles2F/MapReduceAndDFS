package dfs;

import java.io.Serializable;

/*
 * This class is the DFS filesystem's file entry class, it will be extended by directories and files  
 */

public abstract class DFSFileEntry implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7300941993310693871L;
	public String name;
	public abstract String getName();
	public abstract boolean rename(String newName);
	public abstract String getType();
}
