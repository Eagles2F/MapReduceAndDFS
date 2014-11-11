package mapreduce.userlib;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;

import utility.Configuration;

public class Job implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -4325665659967502371L;
	private Class MapperClass;
	private Class ReducerClass;
	private Class CombinerClass;
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
		try {
			System.out.println(conf.getMaster_ip()+" "+conf.getJobSubmission_port());
			Socket soc = new Socket(conf.getMaster_ip(),conf.getJobSubmission_port());
			
			ObjectOutputStream oos = new ObjectOutputStream(soc.getOutputStream());
			
			ObjectInputStream ois = new ObjectInputStream(soc.getInputStream());
			
			oos.writeObject(this);
			//oos.flush();
			System.out.println("SSSSSSSSSSSSS");
			
			int success = ois.readInt();
			
			oos.close();
			ois.close();
			soc.close();
			if(success < 0){
				return false;
			}else{
				return true;
			}
		} catch (IOException e) {
			System.err.println("Socket creation failure!");
			return false;
		}
	}

	public Class getMapperClass() {
		return MapperClass;
	}

	public void setMapperClass(Class mapperClass) {
		MapperClass = mapperClass;
	}

	public Class getReducerClass() {
		return ReducerClass;
	}

	public void setReducerClass(Class reducerClass) {
		ReducerClass = reducerClass;
	}

	public Class getCombinerClass() {
		return CombinerClass;
	}

	public void setCombinerClass(Class combinerClass) {
		CombinerClass = combinerClass;
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
