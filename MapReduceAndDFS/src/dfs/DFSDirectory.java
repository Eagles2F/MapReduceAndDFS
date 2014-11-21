package dfs;

import java.util.concurrent.ConcurrentHashMap;
/*
 * Directory class in the DFS
 */


public class DFSDirectory extends DFSFileEntry{
	private ConcurrentHashMap<String, DFSFileEntry> subEntries; 
	
	public DFSDirectory(String name){
		this.name = name;
		subEntries =new ConcurrentHashMap<String, DFSFileEntry>();
	}
	
	
	//method to create a file entry in the current directory
	public boolean createSubEntry(DFSFileEntry f){
		if(f.getType() != "Directory")
			this.subEntries.put(f.getName(), f);
		return true;
	}
	
	//method to remove a file entry in the current directory
	public boolean removeSubEntry(String name){
		if(subEntries.containsKey(name)){
			this.subEntries.remove(name);
			return true;
		}else{
			System.out.println("Sorry! FileEntry not existed!");
			return false;
		}
	}
	
	//method to get the fileEntry
	public DFSFileEntry getEntry(String name){
		if(subEntries.containsKey(name)){
			return this.subEntries.get(name);
		}else{
			System.out.println("Sorry! FileEntry not existed!");
			return null;
		}
	}
	
	//method to list all the file entries in this directory
	public void ls(){
		for(String key:subEntries.keySet()){
			System.out.println("File: "+subEntries.get(key).getName()+" Type: "+subEntries.get(key).getType());			
		}
		
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
		return "directory";
	}
	
}
