package mapreduce;

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
    
    
    private TaskStatus taskStatus;
    public boolean slotTaken;
    public volatile boolean exit;
    private Thread runningThread;
    private boolean isMapComplete;
    private boolean isConbinComplete;
    private boolean isReduceComplete;
    private String ReducerInputFileName;
    public TaskInstance(Task taskToRun){
        task = taskToRun;
        exit = false;
        taskStatus = new TaskStatus(task.getTaskId());
        reducerNum = task.getReducerNum();
        ReducerInputFileName = task.getReducerInputFileName();
                
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
        Message response=new Message(msgType.RESPONSE);
        response.setResponseId(ResponseType.STARTRES);
        
        response.setTaskId(task.getTaskId());
        if(task.getType() == Task.MAP){
            Class mapperClass;
            try {
                mapperClass = task.getMapClass();
            
                Constructor constructor;
                constructor = mapperClass.getConstructor(null);
                
                MapperRecordWriter rw = new MapperRecordWriter();
                Mapper<Object, Object,Object, Object> process = (Mapper) constructor.newInstance();
                
                RecordReader rr = 
                    new RecordReader(task.getSplit());
                
                
                try {
                    while(!exit && ! isMapComplete){
                        KeyValue<?, ?> keyValuePair = rr.GetNextRecord();
                        if(keyValuePair != null){
                            process.map(keyValuePair.getKey(), keyValuePair.getValue(), rw,task.getTaskId());
                        }
                        else{
                            isMapComplete = true;
                        }
                          
                        
                    }
                    if(exit){
                        taskStatus.setState(TaskStatus.taskState.KILLED);
                        taskComplete();
                    }
                    
                    //combine the output of mapper
                    Class combinerClass = task.getReduceClass();
                    Constructor constructor1;
                    try {
                        constructor1 = combinerClass.getConstructor();
                        CombinerRecordWriter crw = new CombinerRecordWriter(reducerNum);
                        try {
                            Reducer<Object, Iterator<Object>,Object, Object> conbiner = (Reducer) constructor1.newInstance();
                            //use the RecordWriter from the mapper output to the priorirityQueue which store all the map output
                            PriorityQueue<KeyValue> valueQ = rw.getPairQ();
                            Iterator valueItr;
                            while(true){
                                Object currentKey = valueQ.peek().getKey();
                                valueItr = getValueIterator(valueQ);
                                conbiner.reduce(currentKey, valueItr, crw, task.getTaskId());
                                
                            }
                            
                        } catch (InstantiationException | IllegalAccessException
                                | IllegalArgumentException | InvocationTargetException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    } catch (NoSuchMethodException | SecurityException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    
                    
                    
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                
                System.out.println("run process");
                
            }catch (NoSuchMethodException e) {
                response.setResult(Message.msgResult.FAILURE);
                response.setCause("No such method!");
                worker.sendToManager(response);
                return;
            } catch (SecurityException e) {
                response.setResult(Message.msgResult.FAILURE);
                response.setCause("Security Exception!");
                worker.sendToManager(response);
                return;
            } catch (InstantiationException e) {
                response.setResult(Message.msgResult.FAILURE);
                response.setCause("Instantiation Exception!");
                worker.sendToManager(response);
                return;
            } catch (IllegalAccessException e) {
                response.setResult(Message.msgResult.FAILURE);
                response.setCause("Illegal Access !");
                worker.sendToManager(response);
                return;
            } catch (IllegalArgumentException e) {
                response.setResult(Message.msgResult.FAILURE);
                response.setCause("Illegal Argument!");
                worker.sendToManager(response);
                return;
            } catch (InvocationTargetException e) {
                response.setResult(Message.msgResult.FAILURE);
                response.setCause("Invocation Target Exception!");
                worker.sendToManager(response);
                return;
            }
            
            
            
            
        }
        else{
            Class reduceClass;
            try {
                reduceClass = task.getReduceClass();
            
                Constructor constructor;
                constructor = reduceClass.getConstructor(null);
                
                ReducerRecordWriter rw = new ReducerRecordWriter();
                Reducer<Object, Object,Object, Object> process = (Reducer) constructor.newInstance();
                Class<?> inputKeyClass = task.getReduceInputKeyClass();
                Class<?> inputValueClass = task.getReduceInputValueClass();
                
                RecordReader rr = 
                    new RecordReader(task.getSplit());
                
                
                try {
                    PriorityQueue reducerInputQ = sortReducerInput();
                    
                    while(!exit && ! isReduceComplete){
                        
                        Iterator<Object> valueItr;
                        
                        valueItr = getValueIterator(reducerInputQ);
                        if(valueItr == null){
                            isReduceComplete = true;
                            taskStatus.setState(TaskStatus.taskState.COMPLETE);
                            System.out.println("no more value in reducer input");
                        }
                        if(isReduceComplete != true){
                            Object key = ((KeyValue<Object,Object>)reducerInputQ.peek()).getKey();
                            System.out.println("reduce key "+key.toString());
                            process.reduce(((KeyValue<Object,Object>)reducerInputQ.peek()).getKey(),valueItr,rw, task.getTaskId());
                        }
                        
                        
                          
                        
                    }
                    if(exit)
                        taskStatus.setState(TaskStatus.taskState.KILLED);
                    taskComplete();
                    
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                
                System.out.println("run process");
                
            }catch (NoSuchMethodException e) {
                response.setResult(Message.msgResult.FAILURE);
                response.setCause("No such method!");
                worker.sendToManager(response);
                return;
            } catch (SecurityException e) {
                response.setResult(Message.msgResult.FAILURE);
                response.setCause("Security Exception!");
                worker.sendToManager(response);
                return;
            } catch (InstantiationException e) {
                response.setResult(Message.msgResult.FAILURE);
                response.setCause("Instantiation Exception!");
                worker.sendToManager(response);
                return;
            } catch (IllegalAccessException e) {
                response.setResult(Message.msgResult.FAILURE);
                response.setCause("Illegal Access !");
                worker.sendToManager(response);
                return;
            } catch (IllegalArgumentException e) {
                response.setResult(Message.msgResult.FAILURE);
                response.setCause("Illegal Argument!");
                worker.sendToManager(response);
                return;
            } catch (InvocationTargetException e) {
                response.setResult(Message.msgResult.FAILURE);
                response.setCause("Invocation Target Exception!");
                worker.sendToManager(response);
                return;
            }
        }
        
    }
    private void taskComplete() {
        // sedn complete to master
        Message completeMsg = new Message(Message.msgType.INDICATION);
        completeMsg.setIndicationId(IndicationType.TASKCOMPLETE);
        completeMsg.setJobId(task.getJobId());
        completeMsg.setTaskId(task.getTaskId());
        completeMsg.setWorkerID(task.getWorkerId());
        
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
    
    protected Iterator<Object> getValueIterator(PriorityQueue<KeyValue> inputQ){
        ArrayList<Object> valueList = new ArrayList<Object>();
        
        if(inputQ.isEmpty())
            return null;
        KeyValue<Object, Object> keyValuePair = inputQ.peek();
        valueList.add(keyValuePair);
        inputQ.remove();
        if(inputQ.isEmpty()){
            return valueList.iterator();
        }

            
            do{
                KeyValue<Object, Object> keyValuePairNext = inputQ.peek();

                if(keyValuePair.compareTo(keyValuePairNext) == 0){
                    valueList.add(keyValuePairNext.getValue());
                    inputQ.remove();
                }
                else{
                    
                    break;
                }
            }while(true);
        return valueList.iterator();
        
    
   }
    
   private PriorityQueue sortReducerInput(){
       FileInputStream fileStream;
       try {
       fileStream = new FileInputStream(ReducerInputFileName);
       try {
           ObjectInputStream inputStream = new ObjectInputStream(fileStream);
           try {
               PriorityQueue pairQ = new PriorityQueue();
               KeyValue<Object,Object> pair = new KeyValue<Object,Object>();
               while((pair = (KeyValue<Object, Object>) inputStream.readObject()) != null){
                   pairQ.add(pair);
               }
               return pairQ;
               
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