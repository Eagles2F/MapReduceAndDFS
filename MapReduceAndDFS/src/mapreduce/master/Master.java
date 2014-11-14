package mapreduce.master;

import java.util.concurrent.ConcurrentHashMap;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.*;

import mapreduce.WorkerNodeStatus;
import utility.Configuration;

class Master{
	//properties
		
		//The HashMap for worker's socket
		public ConcurrentHashMap<Integer,Socket> workerSocMap;
		public ConcurrentHashMap<Integer,ObjectOutputStream> workerOosMap;
		//The HashMap for each worker's manager server 
		public ConcurrentHashMap<Integer,WorkerManagerServer> workerMangerServerMap;
		
		//The HashMap for worker status
		public ConcurrentHashMap<Integer,WorkerNodeStatus> workerStatusMap;
		
		//The HashMap for received MapReduce jobs
		public ConcurrentHashMap<Integer,MapReduceJob> jobMap; 
		
		
		private HireWorkerServer hireWorkerServer;
		private JobReceiveServer jobReceiverServer;
		
		private Configuration conf;
		
		private BufferedReader console;
		private boolean running;
		 
		public Master(Configuration conf){
			this.conf = conf;
			//initialization for all the fields	
			workerSocMap = new ConcurrentHashMap<Integer,Socket>();
			workerStatusMap = new ConcurrentHashMap<Integer,WorkerNodeStatus>();
			workerMangerServerMap = new ConcurrentHashMap<Integer,WorkerManagerServer>();
			workerOosMap = new ConcurrentHashMap<Integer, ObjectOutputStream>();
			jobMap = new ConcurrentHashMap<Integer, MapReduceJob>();
			console = new BufferedReader(new InputStreamReader(System.in));
			running = true;
			hireWorkerServer = new HireWorkerServer(conf.getHireWorkerServer_port(),this);
			jobReceiverServer = new JobReceiveServer(conf.getJobSubmission_port(),this);
		}
		
	//methods
	
	public void startConsole(){
		        System.out.println("This is mapreduce master, type help for more information");
		        
		        String cmdLine=null;
		        while(running){
		            System.out.print(">>");
		            try{
		                cmdLine = console.readLine();
		                
		            }catch(IOException e){
		                System.out.println("IO error while reading the command,console will be closed");
		            }            
		            
		            String[] inputLine = cmdLine.split(" ");
		           
		            switch(inputLine[0]){
		            	case "ls":
		            		handleLs();
		            		break;
		                default:
		                    System.out.println(inputLine[0]+"is not a valid command");
		            }
		        }
	}
		 
    private void startHireWokerServer(){

        Thread t1 = new Thread(hireWorkerServer);
        t1.start();
    }
    
    private void startJobReceiverServer(){

        Thread t1 = new Thread(jobReceiverServer);
        t1.start();
    }
    
    //console cmd methods
    /*list all the workers and their status*/
    private void handleLs(){
        if(0 == workerStatusMap.size())
            System.out.println("no worker in system");
        else{
            for(int i : workerSocMap.keySet()){
                if(workerStatusMap.get(i).isAlive() == false)
                    System.out.println("worker ID: "+i+"  IP Address: "+workerSocMap.get(i).getInetAddress()+" FAILED");
                else
                    System.out.println("worker ID: "+i+"  IP Address: "+workerSocMap.get(i).getInetAddress()+" ALIVE");
            }
        }
    }
    
	//main process
	public static void main(String[] args){
		//initialization
		 Master master =  new Master(new Configuration("128.237.196.248",11111,11112));
		 
		//start the JobReceiveServer thread
		master.startHireWokerServer();
		//start the HireWorkerServer thread
		master.startJobReceiverServer();
		//start the management shell
		master.startConsole();
	}
}