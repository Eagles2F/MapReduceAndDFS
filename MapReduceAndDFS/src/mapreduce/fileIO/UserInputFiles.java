package mapreduce.fileIO;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
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
	public FileInputFormat fileInputFormat;
	
	public UserInputFiles(FileInputFormat fif){
		fileInputFormat = fif;
	}
	public KeyValue<Object,Object> GetRecordById(int id){
		//Read the number lineNum from the file file_id
		int count =0;
		String line ="";
		try(BufferedReader in = new BufferedReader( new FileReader(this.fileInputFormat.getPath()) )){
			while(true){
				line = in.readLine();
				if(count == id){
					return new KeyValue<Object, Object>(id,line); 
				}
				count++;
			}
		}catch(IOException e){
			e.printStackTrace();
		}
		return null;// if there is no such id, return null
	}
}
