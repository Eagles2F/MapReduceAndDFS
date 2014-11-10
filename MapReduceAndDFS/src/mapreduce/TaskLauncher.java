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
        
        tasksToLaunch.add(taskIns);
        tasksToLaunch.notifyAll();
      }
    }
    
    public void cleanTaskQueue() {
      tasksToLaunch.clear();
    }
    

    
    public void run() {
      while (!Thread.interrupted()) {
        try {
          TaskInstance taskIns;
          synchronized (tasksToLaunch) {
            while (tasksToLaunch.isEmpty()) {
              tasksToLaunch.wait();
            }
            //get the TIP
            taskIns = tasksToLaunch.remove(0);
            
          }
          //wait for a slot to run
          woker.getFreeSlot();

          synchronized (taskIns) {
            //to make sure that there is no kill task action for this
            if (taskIns.getRunState() != TaskStatus.taskState.UNASSIGNED &&
                taskIns.getRunState() != TaskStatus.taskState.FAILED_UNCLEAN &&
                taskIns.getRunState() != TaskStatus.taskState.KILLED_UNCLEAN) {
              //got killed externally while still in the launcher queue
              woker.addFreeSlot();
              continue;
            }
            taskIns.slotTaken = true;
          }
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