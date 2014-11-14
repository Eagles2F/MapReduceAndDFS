package mapreduce;

import java.io.Serializable;

import mapreduce.fileIO.SplitFile;
import mapreduce.userlib.FileOutputFormat;


public class Task implements Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = -7934122013241912385L;
	int taskId;
    TaskStatus taskStatus;
    private int type; // specify the job is a map or a reduce
    public static final int MAP = 0;
    public static final int REDUCE = 1; 
    private int reducerNum;
    private String outputPath;
    
    private String userOutputPath;
    private String reduceTaskInputPath;
    
    public String getReduceTaskInputPath() {
		return reduceTaskInputPath;
	}
	public void setReduceTaskInputPath(String reduceTaskInputPath) {
		this.reduceTaskInputPath = reduceTaskInputPath;
	}
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
  
    private int workerId;
    private int jobId;
    private SplitFile split;
    private String reducerInputFileName;
    
    public String getOutputPath() {
		return outputPath;
	}
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}
    public void setReducerInputFileName(String reducerInputFileName) {
        this.reducerInputFileName = reducerInputFileName;
    }
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
    public SplitFile getSplit() {
        // TODO Auto-generated method stub
        return split;
    }
    public void setSplit(SplitFile s) {
        // TODO Auto-generated method stub
        split =s ;
    }
    
    public void setReducerNum(int num){
        reducerNum = num;
    }
    
    public int getReducerNum(){
        return reducerNum;
    }
    public String getReducerInputFileName() {
        
        return reducerInputFileName;
    }
	public String getUserOutputPath() {
		return userOutputPath;
	}
	public void setUserOutputPath(String userOutputPath) {
		this.userOutputPath = userOutputPath;
	}
}