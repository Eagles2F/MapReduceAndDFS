package mapreduce.master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import mapreduce.TaskStatus;
import mapreduce.TaskStatus.taskState;
import mapreduce.WorkerNodeStatus;
import utility.CommandType;
import utility.IndicationType;
import utility.Message;
import utility.Message.msgResult;
import utility.Message.msgType;

/*
 * This is the specific manager server for each worker. This server will communicate with the worker and handle all
 * the response and state update messages from the worker.
 * @Author: Yifan Li
 * @Author: Jian Wang
 * 
 * @Date: 11/9/2014
 * @Version:0.00,developing version
 */
public class WorkerManagerServer implements Runnable{
    private Master master;
    private int workerId;
    private Socket socket;
    private volatile boolean running;

    private ObjectInputStream objInput;
    private ObjectOutputStream objOutput;

    public WorkerManagerServer(Master master, int id, Socket s) throws IOException {

        this.master = master;
        workerId = id;
        running = true;
        System.out.println("adding a new manager server for worker "+id);
        socket = master.workerSocMap.get(id);
        master.workerStatusMap.put(id, new WorkerNodeStatus(id));//set the initial NodeStatus
        objInput = new ObjectInputStream(socket.getInputStream());
        objOutput = new ObjectOutputStream(socket.getOutputStream());
       
    }
    
    //This method will handle the heartbeat information from the worker to update the status of all the tasks and workers.
    private void handleHeartbeat(WorkerNodeStatus ws){
    	//update the worker status
    	this.master.workerStatusMap.get(ws.getWorkerId()).setAlive(true);
    	this.master.workerStatusMap.get(ws.getWorkerId()).setMaxTask(ws.getMaxTask());
    	//update the tasks status
    	for(int i: ws.getTaskReports().keySet()){
    		TaskStatus ts = ws.getTaskReports().get(i);
    		master.jobMap.get(ts.getJobId()).getMapTaskStatus().set(ts.getTaskId(),ts);
    	}
    	System.out.println("HB:"+ws.getWorkerId());
    }
    
    //This method will handle the worker response to the task start command
    private void handleStartres(Message msg){
    	if(msg.getResult() == msgResult.SUCCESS){
    		//update the task status
    		master.jobMap.get(msg.getJobId()).getMapTaskStatus().get(msg.getTaskId()).setState(taskState.RECEIVED);
    	}else{
    		//update the task status
    		master.jobMap.get(msg.getJobId()).getMapTaskStatus().get(msg.getTaskId()).setState(taskState.FAILED);;
    	}
    }
    
    //This method will handle the task complete infomation. If a MapTask completed, the method will check if all the related
    //tasks are completed or not. If a ReduceTask completed, the method will chekc if all the related tasks are completed or
    //not.
    private void handleTaskcomplete(){
    	
    }
    
    public void run(){
        try{
        	// assign ID to worker
        	Message assignIDmsg = new Message(msgType.COMMAND);
        	assignIDmsg.setCommandId(CommandType.ASSIGNID);
        	assignIDmsg.setWorkerID(workerId);
        	sendToWorker(assignIDmsg);
        	
            Message workerMessage;
            System.out.println("managerServer for worker "+workerId+" running");
            
            //start to listen to the response from the worker
            while(running){
                
            	//receive the msg
                try{
                    workerMessage = (Message) objInput.readObject();
                }catch(ClassNotFoundException e){
                    continue;
                }   
                
                //process the msg
                System.out.println("Worker Msg:"+workerMessage.getWorkerID());
                if(workerMessage.getMessageType() == msgType.RESPONSE){
                	switch(workerMessage.getResponseId()){
                		case STARTRES:
                			handleStartres(workerMessage);
                    	default:
                    		System.out.println("unrecagnized message");
                	}
                }else if(workerMessage.getMessageType() == msgType.INDICATION){
                	switch(workerMessage.getIndicationId()){
                		case HEARTBEAT:
                			handleHeartbeat(workerMessage.getWorkerStatus());
                			break;
                		case TASKCOMPLETE:
                			handleTaskcomplete();
                			break;
                		default:
                			System.out.println("unrecagnized message");
                	}
                }
            }
            objInput.close();
            objOutput.close();
        }catch(IOException e){
          e.printStackTrace();  
        }
    }
    // the send msg method
    public int sendToWorker(Message cmd) throws IOException{
        objOutput.writeObject(cmd);
        objOutput.flush();
        return 0;
}

    
    public void stop(){
        running = false;
    }
}
