/*
 * TaskInstance class is a representative of the the task received from master. Worker will create a taskInstance object
 * for every task received from master and use it to track the task.
 * First, the taskInstatnce will be put into a queue of Task
 * to receive the downlaod request from other dataNode
 * @Author: Yifan Li
 * @Author: Jian Wang
 * 
 * @Date: 11/9/2014
 * @Version:0.00,developing version
 */
package mapreduce;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.PriorityQueue;

import mapreduce.fileIO.RecordReader;
import mapreduce.userlib.Mapper;
import mapreduce.userlib.Reducer;
import utility.CombinerRecordWriter;
import utility.CommandType;
import utility.IndicationType;
import utility.KeyValue;
import utility.MapperRecordWriter;
import utility.Message;
import utility.RecordWriter;
import utility.ReducerRecordWriter;
import utility.ResponseType;
import utility.Message.msgType;


public class TaskInstance implements Runnable{
    private Task task;
    private WorkerNode worker;
    private int reducerNum;
    private int jobId;
    
    public int getJobId() {
		return jobId;
	}
	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

	private TaskStatus taskStatus;
    public boolean slotTaken;
    public volatile boolean exit;
    private Thread runningThread;
    private boolean isMapComplete;
    private boolean isConbinComplete;
    private boolean isReduceComplete;
    private String ReducerInputFileName;
    private String ReducerOutputPath;
    private String mapperOutputPath;
    private String reducerOutputFile;
    public TaskInstance(Task taskToRun, WorkerNode w){
        task = taskToRun;
        exit = false;
        taskStatus = new TaskStatus(task.getTaskId());
        taskStatus.setJobId(task.getJobId());
        reducerNum = task.getReducerNum();
        ReducerInputFileName = task.getReducerInputFileName();
        jobId = task.getJobId();
        mapperOutputPath = task.getOutputPath();
        ReducerOutputPath = task.getUserOutputPath();
        worker = w;
    }
    public TaskStatus.taskState getRunState(){
        return taskStatus.getState();
    }
    
    public void setExit(boolean e){
        exit = e;
    }
    public boolean getExit(){
        return exit;
    }
    public void setRunState(TaskStatus.taskState state){
        taskStatus.setState(state);
        
    }
    
    public TaskStatus.taskPhase getTaskPhase(){
        return taskStatus.getPhase();
    }
    
    public void setProgress(float progress){
        taskStatus.setProgress(progress);
        
    }
    @Override
    public void run() {
        // instantiate the task method
        Message indication=new Message(msgType.INDICATION);
        
        System.out.println("task "+task.getTaskId()+" run");
        
        if(task.getType() == Task.MAP){
            System.out.println("run map task "+task.getTaskId());
            Class<?> mapperClass;
            try {
                mapperClass = task.getMapClass();
            
                Constructor<?> constructor;
                constructor = mapperClass.getConstructor(null);
                
                MapperRecordWriter rw = new MapperRecordWriter();
                Mapper<Object, Object,Object, Object> process = (Mapper<Object, Object, Object, Object>) constructor.newInstance();
                
                RecordReader rr = 
                    new RecordReader(task.getSplit());
                
                
                try {
                    int i=0;
                    while(!exit && ! isMapComplete){
                        i++;
                        KeyValue<?, ?> keyValuePair = rr.GetNextRecord();
                        
                        if(keyValuePair != null){
                            //System.out.println("key "+keyValuePair.getKey().toString());
                            
                            process.map(keyValuePair.getKey(), keyValuePair.getValue(), rw,task.getTaskId());
                        }
                        else{
                            //System.out.println("task "+task.getTaskId()+" "+i);
                            isMapComplete = true;
                        }
                          
                        
                    }
                    if(exit){
                        taskStatus.setState(TaskStatus.taskState.KILLED);
                        
                        indication.setResult(Message.msgResult.FAILURE);
                        indication.setCause("task killed");
                        taskFail(indication);
                    }
                    
                    //combine the output of mapper
                    Class<?> combinerClass = task.getReduceClass();
                    Constructor<?> constructor1;
                    
                    try {
                        constructor1 = combinerClass.getConstructor();
                        CombinerRecordWriter crw = new CombinerRecordWriter(reducerNum,mapperOutputPath,worker.getMapperOutputStream(task.getJobId()), worker);
                        try {
                            Reducer<Object, Iterator<Object>,Object, Object> conbiner = (Reducer<Object, Iterator<Object>, Object, Object>) constructor1.newInstance();
                            //use the RecordWriter from the mapper output to the priorirityQueue which store all the map output
                            PriorityQueue<KeyValue<Object,Object>> valueQ = rw.getPairQ();
                            System.out.println("combine task "+task.getTaskId()+"queue "+valueQ.size());
                            
                            Iterator<Object> valueItr;
                            while(!exit && (valueQ != null) && (valueQ.peek() != null)){
                                Object currentKey = valueQ.peek().getKey();
                                valueItr = getValueIterator(valueQ);
                                //System.out.println("combine key "+currentKey);
                                conbiner.reduce(currentKey, valueItr, crw, task.getTaskId());
                                
                            }
                            if(!exit){
                                taskStatus.setState(TaskStatus.taskState.COMPLETE);
                                
                                
                                taskComplete();
                            }
                            
                        } catch (InstantiationException | IllegalAccessException
                                | IllegalArgumentException | InvocationTargetException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                            indication.setResult(Message.msgResult.FAILURE);
                            indication.setCause("InstantiationException");
                            taskFail(indication);
                        }
                    } catch (NoSuchMethodException | SecurityException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        indication.setResult(Message.msgResult.FAILURE);
                        indication.setCause("NoSuchMethodException");
                        taskFail(indication);
                    }
                    
                    
                    
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    indication.setResult(Message.msgResult.FAILURE);
                    indication.setCause("IOException");
                    taskFail(indication);
                }
                
                System.out.println("run process");
                
            }catch (NoSuchMethodException e) {
                indication.setResult(Message.msgResult.FAILURE);
                indication.setCause("No such method!");
                taskFail(indication);
                return;
            } catch (SecurityException e) {
                indication.setResult(Message.msgResult.FAILURE);
                indication.setCause("Security Exception!");
                taskFail(indication);
                return;
            } catch (InstantiationException e) {
                indication.setResult(Message.msgResult.FAILURE);
                indication.setCause("Instantiation Exception!");
                taskFail(indication);
                return;
            } catch (IllegalAccessException e) {
                indication.setResult(Message.msgResult.FAILURE);
                indication.setCause("Illegal Access !");
                taskFail(indication);
                return;
            } catch (IllegalArgumentException e) {
                indication.setResult(Message.msgResult.FAILURE);
                indication.setCause("Illegal Argument!");
                taskFail(indication);
                return;
            } catch (InvocationTargetException e) {
                indication.setResult(Message.msgResult.FAILURE);
                indication.setCause("Invocation Target Exception!");
                taskFail(indication);
                return;
            }
            
            
            
            
        }
        else{
            Class<?> reduceClass;
            System.out.println("run reducer task "+task.getTaskId());
            taskStatus.setPhase(TaskStatus.taskPhase.REDUCE);
            try {
                reduceClass = task.getReduceClass();
            
                Constructor<?> constructor;
                constructor = reduceClass.getConstructor(null);
                reducerOutputFile = "job"+jobId+"reducer_" + task.getTaskId() +".output";
                File fileToWrite = new File(ReducerOutputPath+"/" + reducerOutputFile);
                
                ReducerRecordWriter rw = new ReducerRecordWriter(ReducerOutputPath,jobId,fileToWrite);
                Reducer<Object, Object,Object, Object> process = (Reducer<Object, Object, Object, Object>) constructor.newInstance();
                
                
                
                
                
                try {
                    PriorityQueue<KeyValue<Object, Object>> reducerInputQ = sortReducerInput();
                    taskStatus.setPhase(TaskStatus.taskPhase.REDUCE);
                    while(!exit && ! isReduceComplete){
                        Object key = null;
                        Iterator<Object> valueItr = null;
                        if(reducerInputQ.peek() != null){
                            key = ((KeyValue<Object,Object>)reducerInputQ.peek()).getKey();
                            valueItr = getValueIterator(reducerInputQ);
                        }
                        if(valueItr == null){
                            isReduceComplete = true;
                            taskStatus.setState(TaskStatus.taskState.COMPLETE);
                            System.out.println("no more value in reducer input");
                        }
                        if((isReduceComplete != true) && (key != null)){
                            
                            System.out.println("reduce key "+key.toString());
                            process.reduce(key,valueItr,rw, task.getTaskId());
                        }
                        
                        
                          
                        
                    }
                    if(exit){
                        taskStatus.setState(TaskStatus.taskState.KILLED);
                        indication.setResult(Message.msgResult.FAILURE);
                        indication.setCause("No such method!");
                        taskFail(indication);
                        
                    }
                    taskComplete();
                    
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    indication.setResult(Message.msgResult.FAILURE);
                    indication.setCause("No such method!");
                    taskFail(indication);
                }
                
                System.out.println("run process");
                
            }catch (NoSuchMethodException e) {
                indication.setResult(Message.msgResult.FAILURE);
                indication.setCause("No such method!");
                taskFail(indication);
                return;
            } catch (SecurityException e) {
                indication.setResult(Message.msgResult.FAILURE);
                indication.setCause("Security Exception!");
                taskFail(indication);
                return;
            } catch (InstantiationException e) {
                indication.setResult(Message.msgResult.FAILURE);
                indication.setCause("Instantiation Exception!");
                taskFail(indication);
                return;
            } catch (IllegalAccessException e) {
                indication.setResult(Message.msgResult.FAILURE);
                indication.setCause("Illegal Access !");
                taskFail(indication);
                return;
            } catch (IllegalArgumentException e) {
                indication.setResult(Message.msgResult.FAILURE);
                indication.setCause("Illegal Argument!");
                taskFail(indication);
                return;
            } catch (InvocationTargetException e) {
                indication.setResult(Message.msgResult.FAILURE);
                indication.setCause("Invocation Target Exception!");
                taskFail(indication);
                return;
            }
        }
        
    }
    private void taskFail(Message indication) {
        worker.addFreeSlot();
        indication.setIndicationId(IndicationType.TASKFAIL);
        indication.setJobId(task.getJobId());
        indication.setTaskId(task.getTaskId());
        indication.setWorkerID(task.getWorkerId());
        indication.setTaskItem(task);
        
        worker.sendToManager(indication);
        
    }
     private void taskComplete() {
        // free the slot and let the queuing task run
        worker.addFreeSlot();
        Message completeMsg = new Message(Message.msgType.INDICATION);
        completeMsg.setIndicationId(IndicationType.TASKCOMPLETE);
        completeMsg.setJobId(task.getJobId());
        completeMsg.setTaskId(task.getTaskId());
        completeMsg.setWorkerID(task.getWorkerId());
        completeMsg.setTaskItem(task);
        completeMsg.setReducerOutputFile(reducerOutputFile);
        
        
        worker.sendToManager(completeMsg);
        
        
    }
    public TaskStatus getTaskStatus() {
        // TODO Auto-generated method stub
        return taskStatus;
    }
    public Task getTask() {
        // TODO Auto-generated method stub
        return task;
    }
    public void setThread(Thread t) {
        // TODO Auto-generated method stub
        runningThread = t;
    }
    
    public Thread getThread(){
        return runningThread;
    }
    
    protected Iterator<Object> getValueIterator(PriorityQueue<KeyValue<Object,Object>> inputQ){
        ArrayList<Object> valueList = new ArrayList<Object>();
        
        if(inputQ.isEmpty())
            return null;
        KeyValue<Object, Object> keyValuePair = inputQ.peek();
        valueList.add(keyValuePair.getValue());
        inputQ.remove();
        if(inputQ.isEmpty()){
            return valueList.iterator();
        }

            
            do{
                KeyValue<Object, Object> keyValuePairNext = inputQ.peek();
                if(keyValuePairNext != null){
                    if(keyValuePair.compareTo(keyValuePairNext) == 0){
                        valueList.add(keyValuePairNext.getValue());
                        inputQ.remove();
                    }
                    else{
                        
                        break;
                    }
                }
                else{
                    break;
                }
                   
            }while(true);
        return valueList.iterator();
        
    
   }
    
   private PriorityQueue<KeyValue<Object, Object>> sortReducerInput(){
       FileInputStream fileStream;
       try {
          System.out.println(mapperOutputPath+"/"+ReducerInputFileName);
       fileStream = new FileInputStream(mapperOutputPath+"/"+ReducerInputFileName);
       try {
           ObjectInputStream inputStream = new ObjectInputStream(fileStream);
           try {
               PriorityQueue<KeyValue<Object, Object>> pairQ = new PriorityQueue<KeyValue<Object, Object>>();
               KeyValue<Object,Object> pair = new KeyValue<Object,Object>();
               try{
               while((pair = (KeyValue<Object, Object>) inputStream.readObject()) != null){
                   System.out.println("sort:"+pair.getKey()+pair.getValue());
                   pairQ.add(pair);
               }
               return pairQ;
               }catch(EOFException e){
                   return pairQ;
               }
               
           } catch (ClassNotFoundException e) {
               // TODO Auto-generated catch block
               e.printStackTrace();
               return null;
           }
       } catch (IOException e) {
           // TODO Auto-generated catch block
           e.printStackTrace();
           return null;
       }
   } catch (FileNotFoundException e) {
       // TODO Auto-generated catch block
       e.printStackTrace();
       return null;
   }
       
   }
 
}