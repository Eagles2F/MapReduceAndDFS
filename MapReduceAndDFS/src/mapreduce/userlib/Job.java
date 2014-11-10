package mapreduce.userlib;

import utility.Configuration;
import mapreduce.fileIO.FileInputFormat;
import mapreduce.fileIO.FileOutputFormat;

public class Job {
	private Class Mapper;
	private Class Reducer;
	private FileInputFormat fif;
	private FileOutputFormat fof;
	private String jobname;
	private Configuration conf;
	
	public Job(String jobname,Configuration conf){
		this.setJobname(jobname);
		this.setConf(conf);
	}
	
	//true-job succeccfully completed. false-job failed for some reason
	public boolean waitForJobCompletion(){
		//submit the job here, send the job object and related class files.
		
		return true;
	}
	
	public Class getMapper() {
		return Mapper;
	}

	public void setMapper(Class mapper) {
		Mapper = mapper;
	}

	public Class getReducer() {
		return Reducer;
	}

	public void setReducer(Class reducer) {
		Reducer = reducer;
	}

	public FileInputFormat getFif() {
		return fif;
	}

	public void setFif(FileInputFormat fif) {
		this.fif = fif;
	}

	public FileOutputFormat getFof() {
		return fof;
	}

	public void setFof(FileOutputFormat fof) {
		this.fof = fof;
	}

	public String getJobname() {
		return jobname;
	}

	public void setJobname(String jobname) {
		this.jobname = jobname;
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}
	
}
