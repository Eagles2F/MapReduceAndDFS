package mapreduce.master;
/*
 * This class represents a MapReduce job which is going to be running on the facility
 */


import java.net.Socket;

public class MapReduceJob {
	private Socket clientSocket;
	
	
	public MapReduceJob(Socket soc){
		this.clientSocket =soc;
	}
	
	public Socket getClientSocket() {
		return clientSocket;
	}

	public void setClientSocket(Socket clientSocket) {
		this.clientSocket = clientSocket;
	}
}
