package mapreduce;

import java.util.HashMap;

public class WorkerNodeStatus{
    private String trackerName;
    private int workerId;
    private int masterAdd;
    private int masterPort;

    private HashMap<Integer,TaskStatus> taskReports;
    private int failures;
    private int maxMapTasks;
    private int maxReduceTasks;
}