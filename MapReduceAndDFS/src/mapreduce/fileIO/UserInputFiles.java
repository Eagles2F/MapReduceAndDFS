package mapreduce.fileIO;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;

import dfs.DFSFile;
import mapreduce.userlib.FileInputFormat;
import utility.KeyValue;
/*
 * This class is the core file I/O class in the MapReduce framework. This file model assumes each file has a fixed 
 * length of files and the number of records per file will be specified by the users.
 *@Author: Yifan Li
 *@Author: Jian Wang
 *
 *@Date: 11/9/2014
 *@Version:0.00,developing version
 */
public class UserInputFiles implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 3507871942167548213L;
	//ordered user files array
	public DFSFile fileChunk;
	public int startId;//the startId of this fileChunk
	public int length;//the length of this fileChunk
	public UserInputFiles(DFSFile file,int start,int len){
		fileChunk = file;
		this.startId = start;
		this.length = len;
	}
	public KeyValue<Object,Object> GetRecordById(int idAbs){
		//transfer the absolute id in the whole input file to the relative id in this file chunk
		int id = idAbs - startId;
		if(id >length){
			return null;
		}
		
		// send the read record request to the NameNode with the DFS path in the fileinputformat.
		
		int count =0;
		String line ="";
		try(BufferedReader in = new BufferedReader( new FileReader(this.fileChunk.getNodeLocalFilePath()+"/"+this.fileChunk.getName()) )){
			while(true){
				line = in.readLine();
				if(line != null){
					if(count == id){
						return new KeyValue<Object, Object>(id,line); 
					}
					count++;
				}else{
					break;
				}				
			}
		}catch(IOException e){
			e.printStackTrace();
		}
		return null;// if there is no such id, return null
	}
}
