package mapreduce.master;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;

import dfs.DFSFile;
import dfs.DFSFile.fileType;
import mapreduce.Task;
import mapreduce.TaskStatus;
import mapreduce.TaskStatus.taskState;
import mapreduce.WorkerNodeStatus;
import utility.ClientMessage;
import utility.CommandType;
import utility.DFSCommandId;
import utility.DFSMessage;
import utility.DFSMessage.nodeType;
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
    private boolean sendGetFileRequest(MapReduceJob job) throws IOException{
		//Simple Scheduling: send the ReducerTask to the worker as long as it is not full.
		// this is a best effort sending, we do not ensure all the tasks must be sent.
			for(int i=0;i<job.getReduceTasks().size();i++){
				Task t=job.getReduceTasks().get(i);
				for(int key: master.workerStatusMap.keySet()){
				if(master.workerStatusMap.get(key).getMaxTask()*2 >
					master.workerStatusMap.get(key).getTaskReports().size()){ //if there is still extra computing ability in the worker node
				  
					job.getReduceTaskStatus().get(i).setWorkerId(key);
					//need to tell the reducer to fetch the file chunk on the mapper
				    DFSMessage dfsMsg = new DFSMessage();
                    dfsMsg.setMessageType(DFSMessage.msgType.COMMAND);
                    dfsMsg.setCmdId(DFSCommandId.GETFILES);
                    dfsMsg.setDownloadType(DFSMessage.DownloadType.OBJECT);
                    
                    dfsMsg.setTargetPath(job.getMapTasks().get(0).getOutputPath());
                    dfsMsg.setTargetFileName("job"+job.getJobId()+"combiner" + i+ ".output");
                    dfsMsg.setLocalPath(job.getMapTasks().get(0).getOutputPath());
                    dfsMsg.setLocalFileName("job"+job.getJobId()+"partitioner" + i+ ".output");
                    dfsMsg.setMessageSource(nodeType.MASTER);
                    dfsMsg.setTaskId(t.getTaskId()); //need send this back when complete. master need this id to track the task
                    t.setReducerInputFileName("job"+job.getJobId()+"partitioner" + i+ ".output");
                    //add the dfs message to a hashMap, in case the worker failure, master could use this def message to recovery
                    job.getDfsMsgConcurrentHashMap().put(t.getTaskId(), dfsMsg);
                    String[] ipAddr = new String[job.getMapTasks().size()];
                    HashMap<Integer,String> addr = new HashMap<Integer,String>();
                    
                    int[] ports = new int[job.getMapTasks().size()];
                    
                    //need send the worker address and port to the worker run reduce
                    int index =0 ;
		            for(int j =0;j< job.getMapTasks().size();j++){
		                //there maybe several mapper tasks on one worker, so only count once for the same worker
		                int workerId = job.getMapTasks().get(j).getWorkerId();
		                System.out.println("task "+job.getMapTasks().get(j).getTaskId()+" workerId"+workerId);
		                if(!addr.containsKey(workerId)){
		                    System.out.println("target address "+master.workerSocMap.get(workerId).getInetAddress().getHostAddress());
		                    addr.put(workerId, master.workerSocMap.get(workerId).getInetAddress().getHostAddress());
		                    
		                    ports[index++] = 21111;
		                }
		            }
		            dfsMsg.setTargetCount(index);
		            dfsMsg.setTargetNodeAddr(addr.values().toArray(ipAddr));
                    dfsMsg.setTargetPortNum(ports);
		            master.getNameNodeServer().getDataNodeManagerMap().get(key).sendToDataNode(dfsMsg);
                    System.out.println(master.workerSocMap.get(key).getInetAddress()+"  "+master.workerSocMap.get(key).getPort()
                            +"task "+ t.getTaskId());
					//goes to the next worker
					break;
				}
				}
		}
    	return true;
    }
    private void sendReduceTask(Message msg){
        
      //send the task the worker with id key
        MapReduceJob job = master.jobMap.get(msg.getJobId());
        Message taskMsg = new Message();
        taskMsg.setMessageType(msgType.COMMAND);
        taskMsg.setCommandId(CommandType.START);
        taskMsg.setJobId(job.getJobId());
        taskMsg.setTaskId(msg.getTaskId());
        
        taskMsg.setTaskItem(job.getReduceTasks().get(msg.getTaskId()));
        try {
            sendToWorker(taskMsg);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }                  
        job.getReduceTaskStatus().get(msg.getTaskId()).setState(taskState.SENT);
    }
    //This method will generate the ReduceTasks
    private boolean genReduceTasks(MapReduceJob job){
    	
    	//Scan the intermediate output path of the job,Get the File List
    	System.out.println("outPut path "+job.getMapTasks().get(0).getOutputPath());
    	for (int i = 0; i < job.getJob().getReducerNum(); i++) {
			Task task = new Task();
			task.setJobId(job.getJobId());
			task.setType(Task.REDUCE);//reducer type
			task.setReducerNum(job.getJob().getReducerNum());
			task.setTaskId(job.getReduceTasks().size());
			task.setReduceClass(job.getJob().getReducerClass());
			//set local output path
			task.setUserOutputPath("../DFS/Output");
			
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
    	this.master.workerStatusMap.get(ws.getWorkerId()).resetNoReportCnt();
    	this.master.workerStatusMap.get(ws.getWorkerId()).setMaxTask(ws.getMaxTask());
    	this.master.workerStatusMap.get(ws.getWorkerId()).setTaskReports(ws.getTaskReports());
    	//update the tasks status
    	for(int i: ws.getTaskReports().keySet()){
    		TaskStatus ts = ws.getTaskReports().get(i);
    		if(ts.getTaskType() == Task.MAP){
    			master.jobMap.get(ts.getJobId()).getMapTaskStatus().set(ts.getTaskId(),ts);
    			//if task failed, send it again
    			if(ts.getState() == TaskStatus.taskState.FAILED){
    			    //clean all the tasks for this job on this worker and resend all the tasks
    			    Message cleanMsg = new Message();
    			    cleanMsg.setMessageType(Message.msgType.COMMAND);
    			    cleanMsg.setCommandId(CommandType.CLEAN);
    			    cleanMsg.setJobId(ts.getJobId());
    			    try {
                        this.sendToWorker(cleanMsg);
                    } catch (IOException e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
    			    //send all tha task on this worker again
    			    for(int k=0;k<master.jobMap.get(ts.getJobId()).getMapTasks().size();k++){
    			        Task mapTask = master.jobMap.get(ts.getJobId()).getMapTasks().get(k);
    			        if(mapTask.getWorkerId() == ts.getWorkerId()){
                			Message recoveryMsg = new Message();
                			recoveryMsg.setMessageType(Message.msgType.COMMAND);
                			recoveryMsg.setCommandId(CommandType.START);
                			recoveryMsg.setTaskItem(mapTask);
                			
                			master.jobMap.get(ts.getJobId()).getMapTaskStatus().set(ts.getTaskId(),ts);
                			try {
                                this.sendToWorker(recoveryMsg);
                            } catch (IOException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
    			        }
    			    }
    			}
    			
    		}else{
    			master.jobMap.get(ts.getJobId()).getReduceTaskStatus().set(ts.getTaskId(), ts);
    			if(ts.getState() == TaskStatus.taskState.FAILED){
                    Message recoveryMsg = new Message();
                    recoveryMsg.setMessageType(Message.msgType.COMMAND);
                    recoveryMsg.setCommandId(CommandType.START);
                    recoveryMsg.setTaskItem(master.jobMap.get(ts.getJobId()).getReduceTasks().get(ts.getTaskId()));
                    ts.setState(TaskStatus.taskState.SENT);
                    master.jobMap.get(ts.getJobId()).getReduceTaskStatus().set(ts.getTaskId(),ts);
                    try {
                        this.sendToWorker(recoveryMsg);
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
    		}
    	}
    	//System.out.println("HB:"+ws.getWorkerId());
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
    		sendGetFileRequest(master.jobMap.get(msg.getJobId()));
    		
    	}
    	}else{ // ReduceTask finished
        	//update the task status
        	master.jobMap.get(msg.getJobId()).getReduceTaskStatus().get(msg.getTaskId()).setState(taskState.COMPLETE);
        	
        	//Create the Output File in the Root directory,the real file is stored on the node
        	String outputFileName = msg.getReducerOutputFile();
        	DFSFile outputFile = new DFSFile(outputFileName);
        	outputFile.setNodeId(msg.getWorkerID());
        	outputFile.setNodeLocalFilePath(msg.getTaskItem().getUserOutputPath());
        	outputFile.setNodeAddress(master.workerSocMap.get(msg.getWorkerID()).getInetAddress().getHostAddress());
        	outputFile.setTypeFile(fileType.TXT);
        	master.getNameNodeServer().getRootDir().createSubEntry(outputFile);
        	
        	//create the replication for the file
        	master.getNameNodeServer().genDup(outputFile);
        	
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
        	   
        		master.jobMap.get(msg.getJobId()).getClientOOS().writeObject(new ClientMessage(1));//succeed!
        		//master.jobMap.remove(msg.getJobId());
        		Message clean = new Message();
        		clean.setMessageType(msgType.COMMAND);
        		clean.setCommandId(CommandType.CLEAN);
        		clean.setJobId(msg.getJobId());
        		clean.setReducerNum(master.jobMap.get(msg.getJobId()).getJob().getReducerNum());
        		for(int key:master.workerMangerServerMap.keySet()){ // send to all the workers to clear the files related to this job
        			master.workerMangerServerMap.get(key).sendToWorker(clean);
        		}
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
        	
            Message workerMessage = null;
            System.out.println("managerServer for worker "+workerId+" running");
            
            //start to listen to the response from the worker
            while(running){
                
            	//receive the msg
                try{
                    workerMessage = (Message) objInput.readObject();
                    //System.out.println("receive "+workerMessage.getMessageType()+" "+workerMessage.getResponseId()+" "+workerMessage.getIndicationId());
                }catch(ClassNotFoundException e){
                    continue;
                }catch(EOFException e){
                    //reach file end, do nothing
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
                		case GETFILESCOMPLETE:
                		    System.out.println("workerManagerServer "+workerId+" receive GetFilesComplete Indication"+
                		"task "+workerMessage.getTaskId());
                		    sendReduceTask(workerMessage);
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
