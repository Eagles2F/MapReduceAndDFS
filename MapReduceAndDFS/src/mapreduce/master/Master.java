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
		            	case "ws":
		            		handleWs();
		            		break;
		            	case "Js":
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
		            	case "cat":
		            		handleCat();
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
    	this.jobMap.get(String.valueOf(cmd[1])).KillTasks();
    	
    	//remove the job form the map
    	this.jobMap.remove(String.valueOf(cmd[1]));
    }
    
    /*quit the whole system including dsf and mapreduce master*/
    private void handleQuit(){
    	
    }
    
    /*Show all the files in the NameNode FileSystem RootDir*/
    private void handleLs(){
    	this.nameNodeServer.getRootDir().ls();
    }
    
    /*show the file's content*/
    private void handleCat(){
    	
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
                
               //set all the tasks still running on this worker to be failed
               for(int i:jobMap.keySet()){
            	   jobMap.get(i).NodeFail(id);
               //send all the tasks on this failed worker to the replica worker
               for(int j:jobMap.keySet()){
                   ArrayList<Task> mapTaskList = jobMap.get(j).getMapTasks();
                   for(int k=0;k<mapTaskList.size();k++){
                       Task mapTask = mapTaskList.get(k);
                       if(mapTask.getWorkerId() == id){
                           //send the task on the failure node to the replica node
                           //find the replica
                           int index = id+1;
                           int replicaId = 0;
                           while(true){
                               replicaId = index++ % hireWorkerServer.getWorkerCnt();
                              if(workerStatusMap.contains(replicaId))
                                  break;
                           }
                           Message recoveryMsg = new Message();
                           recoveryMsg.setTaskItem(mapTask);
                           recoveryMsg.setMessageType(Message.msgType.COMMAND);
                           recoveryMsg.setCommandId(CommandType.START);
                           jobMap.get(i).getMapTaskStatus().get(mapTask.getTaskId()).setState(TaskStatus.taskState.SENT);
                           
                       }
                   }
                   //for reducer task, need to let the recover node to download the mapper output
                   ArrayList<Task> reduceTaskList = jobMap.get(j).getReduceTasks();
                   for(int k=0;k<reduceTaskList.size();k++){
                       Task reduceTask = reduceTaskList.get(k);
                       if(reduceTask.getWorkerId() == id){
                           //send the download message first to the replica node
                           DFSMessage recoveryMsg = new DFSMessage();
                           recoveryMsg = jobMap.get(i).getDfsMsgConcurrentHashMap().get(reduceTask.getTaskId());
                           try {
                            getNameNodeServer().getDataNodeManagerMap().get(id).sendToDataNode(recoveryMsg);
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                           
                       }
                   }
               }
               }
               
            }
            else{
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