package mapreduce;

import java.net.InetAddress;

import utility.RecordReader;

public abstract class Task{
    int taskId;
    TaskStatus taskStatus;
    private int type; // specify the job is a map or a reduce
    public static final int MAP = 0;
    public static final int REDUCE = 1; 

    // get these information from the client submission
    @SuppressWarnings("rawtypes")
    private Class mapClass;
    @SuppressWarnings("rawtypes")
    private Class mapInputKeyClass;
    @SuppressWarnings("rawtypes")
    private Class mapInputValueClass;
    @SuppressWarnings({ "rawtypes", "unused" })
    private Class mapOutputKeyClass;
    @SuppressWarnings({ "rawtypes", "unused" })
    private Class mapOutputValueClass;

    @SuppressWarnings("rawtypes")
    private Class reduceClass;
    @SuppressWarnings("rawtypes")
    private Class reduceInputKeyClass;
    @SuppressWarnings({ "rawtypes", "unused" })
    private Class reduceInputValueClass;
    @SuppressWarnings({ "unused", "rawtypes" })
    private Class reduceOutputKeyClass;
    @SuppressWarnings({ "rawtypes", "unused" })
    private Class reduceOutputValueClass;
    
    // combiner, partitioner, input split
    
    private int workerId;
    private int jobId;
    
    public int getTaskId(){
        return taskId;
    }
    public void setTaskId(int id){
        taskId = id;
    }
    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    
    
    public Class getMapClass() {
        return mapClass;
    }

    public void setMapClass(Class mapClass) {
        this.mapClass = mapClass;
    }

    public Class getMapInputKeyClass() {
        return mapInputKeyClass;
    }

    public void setMapInputKeyClass(Class mapInputKeyClass) {
        this.mapInputKeyClass = mapInputKeyClass;
    }

    public Class getMapInputValueClass() {
        return mapInputValueClass;
    }

    public void setMapInputValueClass(Class mapInputValueClass) {
        this.mapInputValueClass = mapInputValueClass;
    }

    public Class getMapOutputKeyClass() {
        return mapOutputKeyClass;
    }

    public void setMapOutputKeyClass(Class mapOutputKeyClass) {
        this.mapOutputKeyClass = mapOutputKeyClass;
    }

    public Class getMapOutputValueClass() {
        return mapOutputValueClass;
    }

    public void setMapOutputValueClass(Class mapOutputValueClass) {
        this.mapOutputValueClass = mapOutputValueClass;
    }

    public Class getReduceClass() {
        return reduceClass;
    }

    public void setReduceClass(Class reduceClass) {
        this.reduceClass = reduceClass;
    }

    public Class getReduceInputKeyClass() {
        return reduceInputKeyClass;
    }

    public void setReduceInputKeyClass(Class reduceInputKeyClass) {
        this.reduceInputKeyClass = reduceInputKeyClass;
    }

    public Class getReduceInputValueClass() {
        return reduceInputValueClass;
    }

    public void setReduceInputValueClass(Class reduceInputValueClass) {
        this.reduceInputValueClass = reduceInputValueClass;
    }

    public Class getReduceOutputKeyClass() {
        return reduceOutputKeyClass;
    }

    public void setReduceOutputKeyClass(Class reduceOutputKeyClass) {
        this.reduceOutputKeyClass = reduceOutputKeyClass;
    }

    public Class getReduceOutputValueClass() {
        return reduceOutputValueClass;
    }

    public void setReduceOutputValueClass(Class reduceOutputValueClass) {
        this.reduceOutputValueClass = reduceOutputValueClass;
    }
    
    public void setWorkerId(int id){
        workerId = id;
    }
    public int getWorkerId() {
        return workerId;
        
    }
    
    public void setJobId(int id){
        jobId = id;
    }
    public int getJobId() {
        // TODO Auto-generated method stub
        return jobId;
    }
}