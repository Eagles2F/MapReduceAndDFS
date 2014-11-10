package utility;
/*
 * This class offers the configuration parameters loaded from the configuration file.
 */
public class Configuration {
	private int master_ip;
	private int jobSubmission_port;
	private int hireWorkerServer_port;
	
	public Configuration(String conf_file_path){
		//build the Configuration from the conf file
	}
	
	
	public int getMaster_ip() {
		return master_ip;
	}
	public void setMaster_ip(int master_ip) {
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

}
