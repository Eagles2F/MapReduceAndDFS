package mapreduce;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;




import java.util.HashMap;








import utility.CommandType;
import utility.IndicationType;
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


public class WorkerNode {
// properties
	// socket related properties
	private String host;
	private int port;
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
	
	//worker information report 
	WorkerNodeStatus trackStatus;
	WorkerInfoReport workerInfo;
	
    private int freeSlot;
	
// methods
 	//constructing method
	public WorkerNode(){
		this.host = null;
		this.port = 0;
		this.workerID = 0;
		this.freeSlot = 5;
		this.currentTaskMap = new HashMap<Integer, TaskInstance>();
		this.trackStatus= new WorkerNodeStatus();
		this.taskLauncher = new TaskLauncher(this);
	}
	public WorkerNode(String host, int port){
		this.host=host;
		this.port = port;
		this.workerID = 0;
		this.freeSlot = 5;
		this.currentTaskMap = new HashMap<Integer, TaskInstance>();
        this.trackStatus= new WorkerNodeStatus();
        this.taskLauncher = new TaskLauncher(this);
	}
	
	//command handling methods
	private void handle_kill(Message msg) {
		System.out.println("Start to kill the process!");
		//response message prepared!
		Message response = new Message(msgType.RESPONSE);
		/*
		response.setResponseId(ResponseType.KILLRES);
		MigratableProcess mp = currentMap.get(msg.getProcessId());
		currentMap.remove(msg.getProcessId());
		mp.suspend();

		currentMap.remove(mp);
		
		response.setProcessId(mp.getProcessID());
		response.setResult(Message.msgResult.SUCCESS);
		
		//send the response back
		sendToManager(response);		*/
		System.out.println("Killing process finished!");
	}
	
	
	// using reflection to construct the process and find the class by the name of it
	private void handle_start(Message msg) {
		
		System.out.println("Handle start process cmd!");
		// response message prepared!
		Message response=new Message(msgType.RESPONSE);
		response.setResponseId(ResponseType.STARTRES);
		
		response.setTaskId(msg.getTaskId());
		
		TaskInstance taskIns = new TaskInstance(msg.getTask());
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
		if(args.length == 2){
			// establish the connection first.
			String host = args[0];
			int port = Integer.parseInt(args[1]);
			WorkerNode worker = new WorkerNode(host, port);
			
			
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
			
			worker.startreport();
		
			
			//wait for the CMDs and deal with them
			while(!worker.failure){
				try {
					Message master_cmd = (Message) worker.obis.readObject();
					switch(master_cmd.getCommandId()){
						case ASSIGNID:// this command assignes the id to the process
							worker.handle_assignID(master_cmd);
							break;
						case START:// this command tries to start a process on this worker
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
		}else{
			System.out.println("Please enter the host ip and port number");
		}
	}
	
	// worker info method 
	public class WorkerInfoReport implements Runnable{

		@Override
		public void run() {
			// send the info about the current process running information every 5 seconds
			
		    while(!failure){
				Message response=new Message(msgType.INDICATION);
				response.setIndicationId(IndicationType.HEARTBEAT);
				
				
				
				for(int i:currentTaskMap.keySet()){
					TaskInstance taskIns = currentTaskMap.get(i);
					response.getWorkerStatus().getTaskReports().put(taskIns.getTask().getTaskId(), taskIns.getTaskStatus());
				}
				response.getWorkerStatus().setMaxTask(5);
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
    
    
    
}





