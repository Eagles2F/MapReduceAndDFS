package mapreduce.master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import utility.CommandType;
import utility.Message;
import utility.Message.msgType;
import mapreduce.Task;
import mapreduce.userlib.Job;

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
	
	//send the mapTask method
	private void sendMapTasks(MapReduceJob job) throws IOException{
		//Simple Scheduling: send the MapTask to the worker as long as it is not full
		int current_worker = 0;
		for(Task t:job.getMapTasks()){
			while(master.workerStatusMap.get(current_worker).getMaxTask() >
				master.workerStatusMap.get(current_worker).getTaskReports().size()){ //if there is still extra computing ability in the worker node
				//send the task the worker with id current_worker
				ObjectOutputStream oos = new ObjectOutputStream(master.workerSocMap.get(current_worker).getOutputStream());
				Message msg = new Message();
				msg.setMessageType(msgType.COMMAND);
				msg.setCommandId(CommandType.START);
				msg.setJobId(job.getJobId());
				msg.setTaskId(t.getTaskId());
				msg.setTaskItem(t);
				oos.writeObject(msg);
				oos.flush();
				oos.close();
				//goes to the next worker
				current_worker++;
				break;
			}
		}
	}
	
	@Override
	public void run() {
		   try{
	    	   System.out.println("JobReceiveServer is waiting for new MapReduceJobs on the port: "+this.port);
	           while(running){
	               Socket jobSocket = serverSocket.accept();
	               System.out.println("MapReduceJob: "+jobSocket.getInetAddress()+":"+jobSocket.getPort()+" join in");
	    
	               //Parse the received Job submission and Create the job instance on the master
	               ObjectOutputStream oos = new ObjectOutputStream(jobSocket.getOutputStream());
	   			   ObjectInputStream ois = new ObjectInputStream(jobSocket.getInputStream());
	   			   
	   			   //read the job from the input stream
				   Job received_job = (Job)ois.readObject();
	   			   
	               MapReduceJob job = new MapReduceJob(jobSocket,received_job,jobCnt);
	               
	               //Split the job
	               job.Split();
	               //Create the MapTasks
	               job.MapTaskGen();
	               //send the MapTasks away
	               sendMapTasks(job);
	               //store the Mapreduce job into the ConcurrentHashmap
	               master.jobMap.put(jobCnt, job);
	               jobCnt++;
	           } 
	       }catch(IOException e){
	           e.printStackTrace();
	           System.out.println("socket server accept failed");
	       } catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
