package example;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import utility.Configuration;
import utility.RecordWriter;
import mapreduce.userlib.FileInputFormat;
import mapreduce.userlib.FileOutputFormat;
import mapreduce.userlib.Job;
import mapreduce.userlib.Mapper;
/*
 * This is the special example prepared for the MapReduce facility we built. 
 */
import mapreduce.userlib.Reducer;

public class WordCount {

	  public static class TokenizerMapper
	       implements Mapper<Object, String, String, Integer>{
		private String word="";  

	    public void map(Object key, String value, RecordWriter output,int taskId
	    		) throws IOException{
	      StringTokenizer itr = new StringTokenizer(value.toString());
	      while (itr.hasMoreTokens()) {
	        word = itr.nextToken();
	        output.write(word, 1, taskId);
	      }
	    }
	  }

	  public static class IntSumReducer
	       implements Reducer<String,Integer,String,Integer> {
	    private Integer result =0;

	    public void reduce(String key, Iterator<Integer> values,
	    		RecordWriter output,int taskId    
	                       ) throws IOException{
	      int sum = 0;
	       while(values.hasNext()) {
	        sum += values.next();
	      }
	      result = sum;
	      output.write(key, result, taskId);
	    }
	  }

	  public static void main(String[] args){
		 if(args.length != 2){
			 System.out.println("Wrong input parameters");
			 System.out.println("Usage: java exmple.WordCount <intput_file_path> <output_file_path>");
			 System.exit(1);
		 } 
		  
	    Configuration conf = new Configuration("128.237.195.41",11111,11112);
	    Job job = new Job("WordCount",conf);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    int num_records = 50;  //number of records in the input file
	    FileInputFormat fif = new FileInputFormat(args[0],num_records);
	    FileOutputFormat fof = new FileOutputFormat(args[1]);
	    job.setFif(fif);
	    job.setFof(fof);
	    job.setReducerNum(10);
	 	if(job.waitForJobCompletion()){
	 		System.out.println("Job Completed!");
	 	}else{
	 		System.out.println("Job failed due to some reason!");
	 	}
	  }
}
