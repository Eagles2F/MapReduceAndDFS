package mapreduce.fileIO;

import java.io.OutputStream;
import java.util.ArrayList;

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
public class UserInputFiles<K,V> {
	//ordered user files array
	public ArrayList<OutputStream> inputFiles;
	public FileInputFormat fileInputFormat;
	
	public UserInputFiles(FileInputFormat fif){
		fileInputFormat = fif;
		//get the output streams from the fif to the inputFiles
	}
	public KeyValue<K,V> GetRecordById(int id){
		int file_id = (int)Math.floor((double)id/this.fileInputFormat.getSize_per_file());
		int lineNum = id%this.fileInputFormat.getSize_per_file();
		//Read the number lineNum from the file file_id
		return null;
	}
}
