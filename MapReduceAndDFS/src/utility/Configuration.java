package utility;

import java.io.Serializable;
import java.util.Properties;

/*
 * This class offers the configuration parameters loaded from the configuration file.
 */
public class Configuration extends ConfigurationBase implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 988346665358020534L;
	private String master_ip;
	private int jobSubmission_port;
	private int hireWorkerServer_port;
	
	public Configuration(String master_ip,int jobSub_port,int hireWorker_port){
		this.master_ip = master_ip;
		this.jobSubmission_port = jobSub_port;
		this.hireWorkerServer_port = hireWorker_port;
	}
	
	
	public String getMaster_ip() {
		return master_ip;
	}
	public void setMaster_ip(String master_ip) {
		this.master_ip = master_ip;
	}
	public int getJobSubmission_port() {
		return jobSubmission_port;
	}
	public void setJobSubmission_port(int jobSubmission_port) {
		this.jobSubmission_port = jobSubmission_port;
	}
	public int getHireWorkerServer_port() {
		return hireWorkerServer_port;
	}
	public void setHireWorkerServer_port(int hireWorkerServer_port) {
		this.hireWorkerServer_port = hireWorkerServer_port;
	}
	
	public  Properties getProperties(){
	    return prop;
	}

}
