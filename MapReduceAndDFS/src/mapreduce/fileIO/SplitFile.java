package mapreduce.fileIO;

import java.io.Serializable;

/*
 * This class represents the result files after the splitting stage of the user input files. 
 *@Author: Yifan Li
 *@Author: Jian Wang
 *
 *@Date: 11/9/2014
 *@Version:0.00,developing version
 */
public class SplitFile implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1980834077809901298L;
	//the start record ID
	private int startId;
	//the length of this split
	private int length;
	//the input files this split related to
	private UserInputFiles userInputFiles;

	
	public SplitFile(int startId,int length, UserInputFiles uif){
		this.startId = startId;
		this.length = length;
		this.userInputFiles = uif;
	}
	
	public int getStartId() {
		return startId;
	}

	public void setStartId(int startId) {
		this.startId = startId;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public UserInputFiles getUserInputFiles() {
		return userInputFiles;
	}

	public void setUserInputFiles(UserInputFiles userInputFiles) {
		this.userInputFiles = userInputFiles;
	}

}
