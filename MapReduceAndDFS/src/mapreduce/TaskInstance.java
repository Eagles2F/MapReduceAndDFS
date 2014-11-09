package mapreduce;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import utility.CommandType;
import utility.Message;
import utility.RecordReader;
import utility.ResponseType;
import utility.Message.msgType;


public class TaskInstance implements Runnable{
    private Task task;
    private WorkerNode worker;
    
    
    private TaskStatus taskStatus;
    public boolean slotTaken;
    public TaskInstance(Task taskToRun){
        task = taskToRun;
    }
    public TaskStatus.taskState getRunState(){
        return taskStatus.getState();
    }
    @Override
    public void run() {
        // instantiate the task method
        Message response=new Message(msgType.RESPONSE);
        response.setResponseId(ResponseType.STARTRES);
        
        response.setTaskId(task.getTaskId());
        Class mapperClass;
        try {
            mapperClass = task.getMapClass();
        
            Constructor constructor;
            constructor = mapperClass.getConstructor(null);
            
            
            Mapper process = (Mapper) constructor.newInstance(null);
            
            RecordReader rr = task.getRecordReader();
            try {
                while(rr.next()){
                    process.map(rr.createKey(), rr.createValue());
                }
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
    private void taskComplete() {
        // sedn complete to master
        Message completeMsg = new Message(Message.msgType.INDICATION);
        completeMsg.setCommandId(CommandType.TASKCOMPLETE);
        completeMsg.setJobId(task.getJobId());
        completeMsg.setTaskId(task.getTaskId());
        completeMsg.setWorkerID(task.getWorkerId());
        
        worker.sendToManager(completeMsg);
        
        
    }
}