package mapreduce;
public class TaskInstance{
    private Task task;
    private TaskRunner runner;
    private TaskStatus taskStatus;
    public boolean slotTaken;
    public TaskStatus.taskState getRunState(){
        return taskStatus.getState();
    }
}