package mapreduce;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;





public class TaskLauncher extends Thread{

    WorkerNode woker; //query the free slot and run, need to be mutual exlusive
    private List<TaskInstance> tasksToLaunch;

    public TaskLauncher(WorkerNode workerNode) {
      this.woker = workerNode;
      
      this.tasksToLaunch = new LinkedList<TaskInstance>();
      setDaemon(true);
      setName("TaskLauncher for task");
    }

    public void addToTaskQueue(TaskInstance taskIns) {
      synchronized (tasksToLaunch) {
        System.out.println("notify taskLauncher");
        tasksToLaunch.add(taskIns);
        tasksToLaunch.notify();
      }
    }
    
    public void cleanTaskQueue() {
      tasksToLaunch.clear();
    }
    

    
    public void run() {
        System.out.println("taskLauncher run");
      while (!Thread.interrupted()) {
        try {
          TaskInstance taskIns;
          synchronized (tasksToLaunch) {
            while (tasksToLaunch.isEmpty()) {
              tasksToLaunch.wait();
            }
            //get the TIP
            
            taskIns = tasksToLaunch.remove(0);
            System.out.println("taskLauncher get new task "+taskIns.getTask().getTaskId());
            
          }
          //wait for a slot to run
          woker.getFreeSlot();

          
          //got a free slot. launch the task
          startNewTask(taskIns);
        } catch (InterruptedException e) { 
          return; // ALL DONE
        } catch (Throwable th) {
          
        }
      }
    }
    
    private void startNewTask(TaskInstance taskIns) {
        try {
          localizeJob(taskIns);
          taskIns.setRunState(TaskStatus.taskState.RUNNING);
          Thread t = new Thread(taskIns);
         
          taskIns.setThread(t);
          t.setName("task"+taskIns.getTask().getTaskId());
          t.start();
        } catch (Throwable e) {
          
            
          
              //taskIns.kill(true);
              //taskIns.cleanup(true);
          
            
          // Careful! 
          // This might not be an 'Exception' - don't handle 'Error' here!
          if (e instanceof Error) {
            throw ((Error) e);
          }
        }
      }
    
    private void localizeJob(TaskInstance tip) throws IOException {
        
    }
  
}