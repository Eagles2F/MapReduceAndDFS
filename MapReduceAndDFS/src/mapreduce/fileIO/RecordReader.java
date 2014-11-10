package mapreduce.fileIO;
/*
 * This class is used to read the record from the split file.
 *@Author: Yifan Li
 *@Author: Jian Wang
 *
 *@Date: 11/9/2014
 *@Version:0.00,developing version
 */

import utility.KeyValue;

public class RecordReader<K,V> {
	public SplitFile getSplit() {
		return split;
	}

	public void setSplit(SplitFile split) {
		this.split = split;
	}

	public int getCurrent_id() {
		return current_id;
	}

	public void setCurrent_id(int current_id) {
		this.current_id = current_id;
	}

	private SplitFile split;
	
	//the current position of the reader
	private int current_id;
	
	public RecordReader(SplitFile split){
		this.split = split;
		this.current_id = split.getStartId();
	}
	
	
	public KeyValue<?, ?> GetNextRecord(){
		// find the record at the current_id and increment the current_id by 1
		return this.split.getUserInputFiles().GetRecordById(current_id);
	}
}
