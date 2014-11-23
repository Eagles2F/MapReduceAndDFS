package mapreduce.master;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.*;

import dfs.HireDataNodeServer;
import dfs.NameNode;
import mapreduce.Task;
import mapreduce.TaskStatus;
import mapreduce.WorkerNodeStatus;
import mapreduce.fileIO.SplitFile;
import utility.CommandType;
import utility.Configuration;
import utility.DFSMessage;
import utility.Message;

public class Master{
	//properties
		
		//The HashMap for worker's socket
		public ConcurrentHashMap<Integer,Socket> workerSocMap;
		public ConcurrentHashMap<Integer,ObjectOutputStream> workerOosMap;
		//The HashMap for each worker's manager server 
		public ConcurrentHashMap<Integer,WorkerManagerServer> workerMangerServerMap;
		//The HashMap for each worker's manager thread
	    public ConcurrentHashMap<Integer,Thread> workerManagerThreadMap;
		
		//The HashMap for worker status
		public ConcurrentHashMap<Integer,WorkerNodeStatus> workerStatusMap;
		
		//The HashMap for received MapReduce jobs
		public ConcurrentHashMap<Integer,MapReduceJob> jobMap; 
		
		
		private HireWorkerServer hireWorkerServer;
		private JobReceiveServer jobReceiverServer;
		private NameNode         nameNodeServer;
		
		
        private Configuration conf;
		
		private BufferedReader console;
		private boolean running;
        private HireDataNodeServer hireDataNodeServer;
		 
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
			nameNodeServer = new NameNode(conf.getNameNodeServer_port(),this);
			hireDataNodeServer = new HireDataNodeServer(conf.getHireDataNodeServer_port(),nameNodeServer);
			workerManagerThreadMap = new ConcurrentHashMap<Integer,Thread>();
		}
		
	//methods
	public HireWorkerServer getHireWorkerServer() {
			return hireWorkerServer;
	}
	
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
		            	case "ws":
		            		handleWs();
		            		break;
		            	case "js":
		            		handleJs();
		            		break;
		            	case "kill":
		            		handleKill(inputLine);
		            		break;
		            	case "quit":
		            		handleQuit();
		            		break;
		            	case "ls":
		            		handleLs();
		            		break;
		            	case "help":
		            		handleHelp();
		            		break;
		                default:
		                    System.out.println(inputLine[0]+"is not a valid command");
		            }
		        }
	}
		 
	public NameNode getNameNodeServer() {
        return nameNodeServer;
    }

    public void setNameNodeServer(NameNode nameNodeServer) {
        this.nameNodeServer = nameNodeServer;
    }
    
    private void startHireWokerServer(){

        Thread t1 = new Thread(hireWorkerServer);
        t1.start();
    }
    
    private void startHireDataNodeServer(){

        Thread t1 = new Thread(hireDataNodeServer);
        t1.start();
    }
    
    private void startNameNodeServer(){

        Thread t1 = new Thread(nameNodeServer);
        t1.start();
    }
    
    private void startJobReceiverServer(){

        Thread t1 = new Thread(jobReceiverServer);
        t1.start();
    }
    
    //console cmd methods
    /*list all the workers and their status*/
    private void handleWs(){
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
    
    /*list all the jobs and their related tasks status*/
    private void handleJs(){
    	if(this.jobMap.size() == 0){
    		System.out.println("No Job in the system!");
    	}
    	for(int key:this.jobMap.keySet()){
    		System.out.println("Job Status Report for Job: "+jobMap.get(key).getJob().getJobname() + "JobId: "+key);
    		//report each job's task status here
    		this.jobMap.get(key).TaskReport();
    	}
    }
    
    /*kill a specified job*/
    private void handleKill(String[] cmd){
    	if(cmd.length != 2){
    		System.out.println("Usage:kill <job-id>");
    		return ;
    	}
    	//kill the job
    	int jobid=Integer.valueOf(cmd[1]);
    	if(!this.jobMap.containsKey(jobid)){
    		System.out.println("No such job!");
    	}else{
    		this.jobMap.get(jobid).KillTasks();
    	
    		//remove the job form the map
    		this.jobMap.remove(jobid);
    	}
    }
    
    /*quit the whole system including dsf and mapreduce master*/
    private void handleQuit(){
    	System.out.println("<CmdName> <parameters>	<use of the cmd>");
    	System.out.println("ws  show the worker status");
    	System.out.println("ls  show the DFS root directory");
    	System.out.println("js  show the job status");
    	System.out.println("ws  show the worker status");
    	System.out.println("kill <job-id>  kill the specified job");
    	System.out.println("Contact me if any problem you can't solve through email: yifanl@andrew.cmu.edu");
    }
    
    /*Show all the files in the NameNode FileSystem RootDir*/
    private void handleLs(){
    	this.nameNodeServer.getRootDir().ls();
    }
    
    /*show the cmd help*/
    private void handleHelp(){
    	
    }
	//main process
	public static void main(String[] args){
		//initialization
	    Configuration conf = new Configuration();
		 Master master =  new Master(conf);
		 
		//start the JobReceiveServer thread
		master.startHireWokerServer();
		
		//start the NameNode thread
		master.startNameNodeServer();
		
		//start the HireDataNodeServer thread
		master.startHireDataNodeServer();
		
		//start the HireWorkerServer thread
		master.startJobReceiverServer();
		master.startMoniterTimer();
		//start the management shell
		master.startConsole();
	}
	
	/*this function is called by the monitor thread every 5 seconds.
     * it check the status of every worker and increment 
     * the status 1 every time. The ManagerServer will set the status
     * to 0 every time it receive the status report from worker.
     * so when the status number is bigger than 1, it means worker has not
     * update for at least 5 seconds and consider the worker is Failed*/
    private void checkWorkerLiveness(){
        //System.out.println("monitor timer expire!");
        
        Set<Integer> workerIdSet = workerStatusMap.keySet();
        Iterator<Integer> idIterator = workerIdSet.iterator();
        while(idIterator.hasNext()){
            int id = idIterator.next();
           if(workerStatusMap.get(id).getNoReportCnt() > 2){
        	   workerStatusMap.get(id).setAlive(false);
        	   //one worker down
                System.out.println("worker "+id+" is not alive. do reconnect");
                this.workerManagerThreadMap.remove(id);
                this.workerMangerServerMap.remove(id);
                this.workerOosMap.remove(id);
                this.workerSocMap.remove(id);
                this.workerStatusMap.remove(id);
                
                //recover the dataNode on this worker node
                nameNodeServer.handleDataNodeFailure(id);
                
                //recover the mapreduce tasks on this worker node              
                //loop through each job
                for(int i:jobMap.keySet()){
                	jobMap.get(i).NodeFail(id);
                	
                	//recover mapTasks
                	jobMap.get(i).NodeFailMapTaskRecover(id);
                    //recover reduceTasks
                	jobMap.get(i).NodeFailReduceTaskRecover(id);
                } 
            }else{
                workerStatusMap.get(id).incrementNoReportCnt();
            }
                
        }
    }
    /*start a moniter timer which is set to 5 seconds*/
    public void startMoniterTimer(){
        Timer timer = new Timer(true);
        TimerTask task = new TimerTask(){
            public void run(){
                checkWorkerLiveness();
            }
        };
        timer.schedule(task, 0, 5*1000);
        System.out.println("start the monitor timer");
        
    }
}