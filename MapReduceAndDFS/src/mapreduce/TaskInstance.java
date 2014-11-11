package mapreduce;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;

import mapreduce.fileIO.RecordReader;
import mapreduce.userlib.Mapper;
import mapreduce.userlib.Reducer;
import utility.CommandType;
import utility.IndicationType;
import utility.KeyValue;
import utility.Message;
import utility.RecordWriter;
import utility.ResponseType;
import utility.Message.msgType;


public class TaskInstance implements Runnable{
    private Task task;
    private WorkerNode worker;
    
    
    private TaskStatus taskStatus;
    public boolean slotTaken;
    public boolean exit;
    private Thread runningThread;
    private boolean isComplete;
    public TaskInstance(Task taskToRun){
        task = taskToRun;
        exit = false;
        taskStatus = new TaskStatus(task.getTaskId());
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
                
                RecordWriter<?,?> rw = new RecordWriter<Object,Object>();
                Mapper<Object, Object,Object, Object> process = (Mapper) constructor.newInstance();
                Class<?> inputKeyClass = task.getMapInputKeyClass();
                Class<?> inputValueClass = task.getMapInputValueClass();
                
                RecordReader rr = 
                    new RecordReader(task.getSplit());
                
                
                try {
                    while(!exit && ! isComplete){
                        KeyValue<?, ?> keyValuePair = rr.GetNextRecord();
                        if(keyValuePair != null){
                            process.map(keyValuePair.getKey(), keyValuePair.getValue(), (RecordWriter<Object, Object>) rw);
                        }
                        else{
                            isComplete = true;
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
        else{
            Class reduceClass;
            try {
                reduceClass = task.getReduceClass();
            
                Constructor constructor;
                constructor = reduceClass.getConstructor(null);
                
                RecordWriter<?,?> rw = new RecordWriter<Object,Object>();
                Reducer<Object, Object,Object, Object> process = (Reducer) constructor.newInstance();
                Class<?> inputKeyClass = task.getReduceInputKeyClass();
                Class<?> inputValueClass = task.getReduceInputValueClass();
                
                RecordReader rr = 
                    new RecordReader(task.getSplit());
                
                
                try {
                    KeyValue<?, ?> keyValuePair = rr.GetNextRecord();
                    KeyValue<?, ?> keyValuePairPre = rr.GetNextRecord();
                    if(keyValuePair == null)
                        isComplete = true;
                    while(!exit && ! isComplete){
                        keyValuePairPre = keyValuePair;
                        ArrayList valueArray = new ArrayList();
                        
                        do{
                            KeyValue<?, ?> keyValuePairNext = rr.GetNextRecord();
                            if(keyValuePairNext == null){
                                isComplete = true;
                                break;
                            }
                            int tmpHash = keyValuePair.getKey().hashCode();
                            int tmpHashNext = keyValuePairNext.getKey().hashCode();
                            if(tmpHash == tmpHashNext)
                                valueArray.add(keyValuePairNext.getValue());
                            else{
                                keyValuePair = keyValuePairNext;
                                break;
                            }
                            
                        }while(true);
                        if(isComplete != true){
                            process.reduce(keyValuePairPre.getKey(), valueArray.iterator(), (RecordWriter<Object, Object>) rw);
                        }
                        else{
                            isComplete = true;
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
}