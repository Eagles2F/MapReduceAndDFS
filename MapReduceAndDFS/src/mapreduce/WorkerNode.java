package mapreduce;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

import utility.CommandType;
import utility.IndicationType;
import utility.KeyValue;
import utility.Message;

/*
 * The workerNode is the slave of the process manager. It is a seperate running machine which is connected 
 * to the process manager with socket. When it is started, the port and IP address of the processmanager
 * is set.  Its responsibilities including:
 * 	Response to the CMD from the process manager.
 *  send message every 5 seconds to the process manager to inform its own status. 
 *  
 *  @Author Yifan Li
 *  @Author Jian Wang
 **/
import utility.Message.msgType;
import utility.ResponseType;
import utility.WorkerConfig;


public class WorkerNode {
// properties
	// socket related properties
	private String host;
	//private int port;
	private Socket socket;
	private TaskLauncher taskLauncher;
	
	// object I/O port
	private ObjectInputStream obis;
	private ObjectOutputStream obos;

	private boolean failure = false; // failure == ture means the process died!
	private int workerID;
	// process related properties
	private Thread t;
	private HashMap<Integer, TaskInstance> currentTaskMap;
	private HashMap<Integer, ArrayList<ObjectOutputStream>> mapperOutputStreamMap;
	
	//worker information report 
	WorkerNodeStatus trackStatus;
	WorkerInfoReport workerInfo;
	
	//worker configuration, read from the property file
	WorkerConfig config;
	
    private int freeSlot;
    private int hostPort;
    
	
// methods
 	//constructing method
	public WorkerNode(){
	    config = new WorkerConfig();
	    
		this.host = config.getMasterAdd();
		this.hostPort = Integer.valueOf(config.getMasterPort());
	    
		this.workerID = 0;
		this.freeSlot = 5;
		this.currentTaskMap = new HashMap<Integer, TaskInstance>();
		this.trackStatus= new WorkerNodeStatus();
		this.taskLauncher = new TaskLauncher(this);
		this.workerInfo = new WorkerInfoReport();
		
		this.mapperOutputStreamMap = new HashMap<Integer, ArrayList<ObjectOutputStream>>();
	}
	public WorkerNode(String host, int port){
		this.host=host;
		this.hostPort = port;
		this.workerID = 0;
		this.freeSlot = 5;
		this.currentTaskMap = new HashMap<Integer, TaskInstance>();
        this.trackStatus= new WorkerNodeStatus();
        this.taskLauncher = new TaskLauncher(this);
        this.workerInfo = new WorkerInfoReport();
        
        this.mapperOutputStreamMap = new HashMap<Integer, ArrayList<ObjectOutputStream>>();
	}
	
	//command handling methods
	private void handle_kill(Message msg) {
		System.out.println("Start to kill the process!");
		//response message prepared!
		Message response = new Message(msgType.RESPONSE);
		
		response.setResponseId(ResponseType.KILLTASKRES);
		TaskInstance taskIns = currentTaskMap.get(msg.getTaskId());
		taskIns.setExit(true);
		
		
		response.setTaskId(taskIns.getTask().getTaskId());
		response.setResult(Message.msgResult.SUCCESS);
		
		//send the response back
		sendToManager(response);		
		System.out.println("Killing process finished!");
	}
	
	
	// using reflection to construct the process and find the class by the name of it
	private void handle_start(Message msg) {
		
		System.out.println("Handle start process cmd!");
		// response message prepared!
		Message response=new Message(msgType.RESPONSE);
		response.setResponseId(ResponseType.STARTRES);
		
		response.setTaskId(msg.getTaskId());
		response.setTaskItem(msg.getTask());
		int jobId = msg.getTask().getJobId();
		if(!mapperOutputStreamMap.containsKey(jobId)){
		    ArrayList<ObjectOutputStream> mapperOutputList = new ArrayList<ObjectOutputStream>();
		    for(int i = 0;i<msg.getTask().getReducerNum();i++){
    		    File fileToWrite = new File("../Output/Intermediate/"+ "job"+msg.getTask().getJobId()+"combiner" + i+ ".output");
    		    try {
    	          if (fileToWrite.exists() == false) {
    
    	              fileToWrite.createNewFile();
    
    	          }
    	          
    	          FileOutputStream fileStream = new FileOutputStream(fileToWrite, true);
    	          ObjectOutputStream outputStream = new ObjectOutputStream(fileStream);
    	          mapperOutputList.add(outputStream);
 

	          
	      } catch (IOException e) {
	          // TODO Auto-generated catch block
	          e.printStackTrace();
	      }
		    }
		    mapperOutputStreamMap.put(msg.getTask().getJobId(), mapperOutputList);
		}
		
		TaskInstance taskIns = new TaskInstance(msg.getTask(),this);
		taskIns.setRunState(TaskStatus.taskState.QUEUING);
		currentTaskMap.put(msg.getTaskId(), taskIns);
		
		
		taskLauncher.addToTaskQueue(taskIns);
		
		// send the response back to the master
		response.setResult(Message.msgResult.SUCCESS);
		
		sendToManager(response);
		System.out.println("Process has been started!");
	}
	
	 // handle the command assign id 
	private void handle_assignID(Message msg){
		this.workerID =msg.getWorkerID(); 
	}
	
	// handle the command clear
	private void  handle_clear(Message msg){
	    /*
		this.currentMap.remove(msg.getProcessId());
		System.out.println("Process:"+msg.getProcessId()+"is cleared!");
		*/
	}
	
	// hanld the command exit
	
	private void handle_exit(Message msg){
		System.out.println("shutdown!");
		System.exit(0);
	}
	private void startreport(){
	    
		Thread t1 = new Thread(workerInfo);
		t1.start();
	}
	
	// some auxiliary methods
	    /*
		// create a thread to run the process
		private  void runProcess(MigratableProcess mp) {
			System.out.println("start process");
			t = new Thread(mp);
			t.start();
			mp.setStatus(ProcessInfo.Status.RUNNING);
			System.out.println("mg getProcessID "+mp.getProcessID());
			currentMap.put(mp.getProcessID(), mp);
		}
		*/
		
		//send method writes object into output stream
		public void sendToManager(Message sc) {
			try {
				obos.writeObject(sc);
			} catch (IOException e) {
			    failure = true;
				System.err.println("fail to send manager");
			}
		}
		
		
	public static void main(String [] args){
		//start only when there are two arguments 
		
	        WorkerNode worker = new WorkerNode();
			String host = worker.getHost();
			int port = worker.getHostPort();
			
			
			
			try{
				worker.socket = new Socket(host,port);
			}catch(IOException e){
				worker.failure = true;
				System.err.println("Socket creation failure!");
				System.exit(0);
			}
			System.out.println("Socket creation succeded!");
			
			//establish the object IO tunnel

			try {
				worker.obos = new ObjectOutputStream(
						worker.socket.getOutputStream());
				worker.obis = new ObjectInputStream(
						worker.socket.getInputStream());
			} catch (IOException e) {
				worker.failure = true;
				System.err.println("cannot create stream");
				e.printStackTrace();
			}

			//worker info backend started
			System.out.println("Start report!");
			worker.startreport();
			worker.startTaskLauncher();
			
		
			
			//wait for the CMDs and deal with them
			while(!worker.failure){
				try {
					Message master_cmd = (Message) worker.obis.readObject();
					System.out.println("receive message: "+master_cmd.getCommandId());
					switch(master_cmd.getCommandId()){
						case ASSIGNID:// this command assignes the id to the process
							worker.handle_assignID(master_cmd);
							break;
						case START:// this command tries to start a process on this worker
						    System.out.println("start task "+master_cmd.getTask().getTaskId()+" "+master_cmd.getTask().getType());
							worker.handle_start(master_cmd);
							break;
						
						case KILLTASK:	// this command tries to kill a process on this worker
							worker.handle_kill(master_cmd);
							break;
						case SHUTDOWN:// this command shutdown this worker
							worker.handle_exit(master_cmd);
							break;
						default:
							System.out.println("Wrong cmd:"+master_cmd.getCommandId());
							break;
					}
					
				} catch (ClassNotFoundException e) {
					worker.failure = true;
					System.out.println("Class not found!");
				} catch (IOException e) {
					worker.failure = true;
					System.out.println("Cannot read from the stream!");
				}
			}
			try {
				worker.obos.close();
				worker.obis.close();
				worker.socket.close();
				System.out.println("Process Worker closed");
			} catch (IOException e) {
				e.printStackTrace();
			}
		
	}
	
	private void startTaskLauncher() {
        Thread t = new Thread(taskLauncher);
        t.start();
        
    }
    private int getHostPort() {
        
        return hostPort;
    }
    private String getHost() {
        // TODO Auto-generated method stub
        return host;
    }

    // worker info method 
	public class WorkerInfoReport implements Runnable{

		@Override
		public void run() {
			// send the info about the current process running information every 5 seconds
			System.out.println("worker report");
		    while(!failure){
				Message response=new Message(msgType.INDICATION);
				response.setIndicationId(IndicationType.HEARTBEAT);
				
				response.setWorkerID(workerID);
				
				for(int i:currentTaskMap.keySet()){
					TaskInstance taskIns = currentTaskMap.get(i);
					//for all running task, query the thread status
					if(taskIns.getThread() != null){
    					if((taskIns.getThread().getState() == Thread.State.TERMINATED) &&
    					        (taskIns.getRunState() != TaskStatus.taskState.COMPLETE))
    					    taskIns.getTaskStatus().setState(TaskStatus.taskState.FAILED);
    					
    					}
					taskIns.getTaskStatus().setTaskType(taskIns.getTask().getType());
					response.getWorkerStatus().getTaskReports().put(taskIns.getTask().getTaskId(), taskIns.getTaskStatus());
				
				}
				WorkerNodeStatus ws = response.getWorkerStatus();
				ws.setMaxTask(100);
				sendToManager(response);
				
				try {
					Thread.sleep(5 * 1000);
				} catch (InterruptedException e) {
				
				}
			}
			
		}
		
	}
	
	

    synchronized public void getFreeSlot() {
        // every task launcher need to call this to get a free slot to launch
        // the task. If no free slot, this call will wait
        while(freeSlot < 1){
            try {
                wait();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
        freeSlot--;
        return;
            
        
    }
    synchronized public void addFreeSlot() {
        // every task should call this in the runner onComplete
        freeSlot++;
        notify();
    }
    public ArrayList<ObjectOutputStream> getMapperOutputStream(int jobId) {
        
        return mapperOutputStreamMap.get(jobId);
    }
    
    
    
}





