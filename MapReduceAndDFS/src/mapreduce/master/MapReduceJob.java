package mapreduce.master;
/*
 * This class represents a MapReduce job which is going to be running on the facility
 */


import java.net.Socket;

import mapreduce.userlib.Job;

public class MapReduceJob {
	private Socket clientSocket;
	private Job job;
	
	public MapReduceJob(Socket soc,Job job){
		this.clientSocket =soc;
		this.job = job;
	}
	
	public Socket getClientSocket() {
		return clientSocket;
	}

	public void setClientSocket(Socket clientSocket) {
		this.clientSocket = clientSocket;
	}

	public Job getJob() {
		return job;
	}

	public void setJob(Job job) {
		this.job = job;
	}
}
