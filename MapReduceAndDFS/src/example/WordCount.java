package example;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

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

      static ClientConfig config;
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
	  public WordCount(){
          config = new ClientConfig();
          
          config.getMasterAdd();
          Integer.valueOf(config.getMasterPort());
      }
	  public static class IntSumReducer
	       implements Reducer<String,Integer,String,Integer> {
	    private Integer result =0;

	    public void reduce(String key, Iterator<Integer> values,
	    		RecordWriter output,int taskId    
	                       ) throws IOException{
	      int sum = 0;
	      
	       while(values.hasNext()) {
	           int i = values.next();
	           
	        //sum += values.next();
	           sum += i;
	      }
	       System.out.println("workd count reducer key "+key+" "+sum);
	      result = sum;
	      output.write(key, result, taskId);
	    }
	  }

	  public static void main(String[] args) throws ClassNotFoundException{
		 if(args.length != 2){
			 System.out.println("Wrong input parameters");
			 System.out.println("Usage: java exmple.WordCount <intput_file_path> <output_file_path>");
			 System.exit(1);
		 } 

		 config = new ClientConfig();
         
         config.getMasterAdd();
         Integer.valueOf(config.getMasterPort());  

	    Job job = new Job("WordCount",config);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setReducerNum(4);
	    int num_records = Integer.valueOf(config.getRecordNum());  //number of records in the input file
	    FileInputFormat fif = new FileInputFormat(args[0],num_records);
	    FileOutputFormat fof = new FileOutputFormat(args[1]);
	    job.setFif(fif);
	    job.setFof(fof);
	 	if(job.waitForJobCompletion()){
	 		System.out.println("Job Completed!");
	 	}else{
	 		System.out.println("Job failed due to some reason!");
	 	}
	  }
}
