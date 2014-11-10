package mapreduce.master;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/*
 * This server is responsible to listen to the job submit request from the MapReduce client and add the Job to
 * the jobMap in master. The job will be split into MapTask and send to the workers.   
 *@Author: Yifan Li
 *@Author: Jian Wang
 *
 *@Date: 11/9/2014
 *@Version:0.00,developing version
 */
public class JobReceiveServer implements Runnable{

	private Master master;
	
	//the port this server is listening to 
	private int port; 
	
	//count the number of the jobs requested
	private int jobCnt;
	
	private volatile boolean running;
	ServerSocket serverSocket;
	
	public JobReceiveServer(int port, Master master){
		this.port = port;
		this.master = master;
		running =true;
		jobCnt = 0;
		try {
		      serverSocket = new ServerSocket(port);
		      } catch (IOException e) {
		        e.printStackTrace();
		        System.out.println("failed to create the socket server");
		        System.exit(0);
		}
		System.out.println("create JobReceiverServer");
	}
	
	
	@Override
	public void run() {
		   try{
	    	   System.out.println("waiting for new MapReduceJobs");
	           while(running){
	               Socket jobSocket = serverSocket.accept();
	               System.out.println("MapReduceJob: "+jobSocket.getInetAddress()+":"+jobSocket.getPort()+" join in");
	              
	               //Parse the received Job submission and Create the job instance on the master
	               MapReduceJob job = new MapReduceJob(jobSocket); 
	               master.jobMap.put(jobCnt, job);
	               jobCnt++;
	               
	               //Split the job
	               
	               //Create the MapTasks
	               
	               //send the MapTasks away
	           } 
	       }catch(IOException e){
	           e.printStackTrace();
	           System.out.println("socket server accept failed");
	       }
	       try {
	        serverSocket.close();
	       } catch (IOException e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
	        System.out.println("socket Server failed to close");
	    }
	        
		
	}
	
	public void stop(){
		running = false;
	}
}
