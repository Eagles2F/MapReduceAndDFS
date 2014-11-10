package mapreduce.master;

import java.util.concurrent.ConcurrentHashMap;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;

import utility.Configuration;

class Master{
	//properties
		
		//The HashMap for worker's socket
		public ConcurrentHashMap<Integer,Socket> workerSocMap;
		
		//The HashMap for each worker's manager server 
		public ConcurrentHashMap<Integer,WorkerManagerServer> workerMangerServerMap;
		
		//The HashMap for worker status:-1 represents "dead", 1 represents "alive so far".
		public ConcurrentHashMap<Integer,Integer> workerStatusMap;
		
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
			console = new BufferedReader(new InputStreamReader(System.in));
			running = true;
		}
		
	//methods
	
	public void startConsole(){
		        System.out.println("This is mapreduce master, type help for more information");
		        
		        String cmdLine=null;
		        while(true){
		            System.out.print(">>");
		            try{
		                cmdLine = console.readLine();
		                
		            }catch(IOException e){
		                System.out.println("IO error while reading the command,console will be closed");
		            }            
		            
		            String[] inputLine = cmdLine.split(" ");
		           
		            switch(inputLine[0]){
		        
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
    
	//main process
	public static void main(String[] args){
		//initialization
		 if(args.length != 1){
	            System.out.println("wrong arguments, usage: Master <configuration file directory>");
	            return;
	     }
		 Master master =  new Master(new Configuration(args[0]));
		 
		//start the JobReceiveServer thread
		master.startHireWokerServer();
		//start the HireWorkerServer thread
		master.startJobReceiverServer();
		//start the management shell
		master.startConsole();
	}
}