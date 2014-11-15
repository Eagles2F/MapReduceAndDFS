package mapreduce.master;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import mapreduce.Task;
import mapreduce.TaskStatus;
import mapreduce.TaskStatus.taskState;
import mapreduce.WorkerNodeStatus;
import utility.CommandType;
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
        master.workerOosMap.put(id, objOutput);//add the OOS to the map
     
    }
    
    //This method will send the reduce tasks
    private boolean sendReduceTasks(MapReduceJob job) throws IOException{
		//Simple Scheduling: send the ReducerTask to the worker as long as it is not full.
		// this is a best effort sending, we do not ensure all the tasks must be sent.
			for(int i=0;i<job.getReduceTasks().size();i++){
				Task t=job.getReduceTasks().get(i);
				for(int key: master.workerStatusMap.keySet()){
				if(master.workerStatusMap.get(key).getMaxTask() >
					master.workerStatusMap.get(key).getTaskReports().size()){ //if there is still extra computing ability in the worker node
					//send the task the worker with id key
					System.out.println(master.workerSocMap.get(key).getInetAddress()+"  "+master.workerSocMap.get(key).getPort());
					Message msg = new Message();
					msg.setMessageType(msgType.COMMAND);
					msg.setCommandId(CommandType.START);
					msg.setJobId(job.getJobId());
					msg.setTaskId(t.getTaskId());
					msg.setTaskItem(t);
					master.workerOosMap.get(key).writeObject(msg);					
					job.getReduceTaskStatus().get(i).setState(taskState.SENT);
					//goes to the next worker
					break;
				}
				}
		}
    	return true;
    }
    //This method will generate the ReduceTasks
    private boolean genReduceTasks(MapReduceJob job){
    	
    	//Scan the intermediate output path of the job,Get the File List
    	File folder = new File(job.getMapTasks().get(0).getOutputPath());
    	System.out.println("outPut path "+job.getMapTasks().get(0).getOutputPath());
    	File[] listOfFiles = folder.listFiles();
    	for (int i = 0; i < listOfFiles.length; i++) {
			Task task = new Task();
			task.setJobId(job.getJobId());
			task.setType(Task.REDUCE);//reducer type
			task.setReducerNum(job.getJob().getReducerNum());
			task.setTaskId(job.getReduceTasks().size());
			task.setReduceClass(job.getJob().getReducerClass());
			//set file IO
			task.setUserOutputPath(job.getJob().getFof().getPath());
			task.setReducerInputFileName(listOfFiles[i].getName());
			task.setOutputPath(job.getMapTasks().get(0).getOutputPath());
			job.getReduceTasks().add(task);
			TaskStatus taskStatus = new TaskStatus(task.getTaskId());
			taskStatus.setTaskType(Task.REDUCE);
			job.getReduceTaskStatus().add(taskStatus);
    	}
    	return true;
    }
    
    //This method will handle the heartbeat information from the worker to update the status of all the tasks and workers.
    private void handleHeartbeat(WorkerNodeStatus ws){
    	//update the worker status
    	this.master.workerStatusMap.get(ws.getWorkerId()).setAlive(true);
    	this.master.workerStatusMap.get(ws.getWorkerId()).setMaxTask(ws.getMaxTask());
    	//update the tasks status
    	for(int i: ws.getTaskReports().keySet()){
    		TaskStatus ts = ws.getTaskReports().get(i);
    		if(ts.getTaskType() == Task.MAP){
    			master.jobMap.get(ts.getJobId()).getMapTaskStatus().set(ts.getTaskId(),ts);
    		}else{
    			master.jobMap.get(ts.getJobId()).getReduceTaskStatus().set(ts.getTaskId(), ts);
    		}
    	}
    	System.out.println("HB:"+ws.getWorkerId());
    }
    
    //This method will handle the worker response to the task start command
    private void handleStartres(Message msg){
    	if(msg.getResult() == msgResult.SUCCESS){
    		//update the task status
    		if(msg.getTask().getType() == 0){
    			master.jobMap.get(msg.getJobId()).getMapTaskStatus().get(msg.getTaskId()).setState(taskState.RECEIVED);
    		}else{
    			master.jobMap.get(msg.getJobId()).getReduceTaskStatus().get(msg.getTaskId()).setState(taskState.RECEIVED);
    		}
    		
    	}else{
    		//update the task status
    		if(msg.getTask().getType() == 0){
    			master.jobMap.get(msg.getJobId()).getMapTaskStatus().get(msg.getTaskId()).setState(taskState.FAILED);
    		}else{
    			master.jobMap.get(msg.getJobId()).getReduceTaskStatus().get(msg.getTaskId()).setState(taskState.FAILED);
    		}
    	}
    }
    
    //This method will handle the task complete information. If a MapTask completed, the method will check if all the related
    //tasks are completed or not. If a ReduceTask completed, the method will check if all the related tasks are completed or
    //not.
    private void handleTaskcomplete(Message msg) throws IOException{
    	System.out.println("COMPLETE!");
    	if(msg.getTask().getType() == 0){//Map task finished
 	
    	//update the task status
    	master.jobMap.get(msg.getJobId()).getMapTaskStatus().get(msg.getTaskId()).setState(taskState.COMPLETE);
    	
    	//check whether all the tasks in the same job has finished
    	boolean finished = true;
    	for(TaskStatus ts:master.jobMap.get(msg.getJobId()).getMapTaskStatus()){
    		if(ts.getState() != taskState.COMPLETE){
    			finished = false;
    		}
    	}
    	
    	//if all the tasks have been finished, started to build the reduce task and send them.
    	if(finished == true){
    		if(!genReduceTasks(master.jobMap.get(msg.getJobId()))){
    			System.out.println("Fail to Gen Reduce tasks!");
    		}
    		if(!sendReduceTasks(master.jobMap.get(msg.getJobId()))){
    			System.out.println("Fail to Send Reduce tasks!");
    		}
    	}
    	}else{ // ReduceTask finished
        	//update the task status
        	master.jobMap.get(msg.getJobId()).getReduceTaskStatus().get(msg.getTaskId()).setState(taskState.COMPLETE);
     
        	//check whether all the tasks in the same job has finished
        	boolean finished = true;
        	for(TaskStatus ts:master.jobMap.get(msg.getJobId()).getReduceTaskStatus()){
        	    System.out.println("in complete: task "+ts.getTaskId()+" "+ts.getState());
        		if(ts.getState() != taskState.COMPLETE){
        			finished = false;
        		}
        	}
        	//if all the tasks have been finished, started to build the reduce task and send them.
        	if(finished == true){
        	    System.out.println("job finished, send cfm");
        		//send the message to the job client with a success
        		master.jobMap.get(msg.getJobId()).getClientOOS().writeObject(Integer.getInteger("1"));//succeed!
        	}
    	}   	
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
                    System.out.println("receive "+workerMessage.getMessageType()+" "+workerMessage.getResponseId()+" "+workerMessage.getIndicationId());
                }catch(ClassNotFoundException e){
                    continue;
                }   
                
                //process the msg
               
                workerMessage.getResponseId();
                if(workerMessage.getMessageType() == msgType.RESPONSE){
                	switch(workerMessage.getResponseId()){
                		case STARTRES:
                			handleStartres(workerMessage);
                			break;
                    	default:
                    		System.out.println("unrecagnized message");
                	}
                }else if(workerMessage.getMessageType() == msgType.INDICATION){
                	switch(workerMessage.getIndicationId()){
                		case HEARTBEAT:
                			handleHeartbeat(workerMessage.getWorkerStatus());
                			break;
                		case TASKCOMPLETE:
                			handleTaskcomplete(workerMessage);
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
