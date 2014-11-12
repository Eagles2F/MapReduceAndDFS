package mapreduce.fileIO;

import java.io.OutputStream;
import java.util.ArrayList;

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
public class UserInputFiles{
	//ordered user files array
	public OutputStream inputFiles;
	public FileInputFormat fileInputFormat;
	
	public UserInputFiles(FileInputFormat fif){
		fileInputFormat = fif;
		//get the output streams from the fif to the inputFiles
		
	}
	public KeyValue<Object,Object> GetRecordById(int id){
		//Read the number lineNum from the file file_id
		return null;
	}
}
