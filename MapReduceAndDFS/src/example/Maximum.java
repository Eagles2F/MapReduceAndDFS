package example;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import example.WordCount.IntSumReducer;
import example.WordCount.TokenizerMapper;
import utility.RecordWriter;
import mapreduce.userlib.FileInputFormat;
import mapreduce.userlib.FileOutputFormat;
import mapreduce.userlib.Job;
import mapreduce.userlib.Mapper;
import mapreduce.userlib.Reducer;

public class Maximum {
	static ClientConfig config;
	  public Maximum(){
          config = new ClientConfig();
          
          config.getMasterAdd();
          Integer.valueOf(config.getMasterPort());
      }
	public static class MaxMap implements
			Mapper<Integer, String, String, Integer> {
		private final static String max = "max";
		private Integer number;

		public void map(Integer key, String value, RecordWriter outPut,
				int taskId) throws IOException {
			String line = value;
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
					number = Integer.valueOf(tokenizer.nextToken());
				
				outPut.write(max, number,taskId);
			}
			
		}

	}	
	 public static class  MaxReduce
     	implements Reducer<String,Integer,String,Integer> {
		 public void reduce(String key, Iterator<Integer> values,
				 RecordWriter output,int taskId    
                     ) throws IOException{
	   int maxValue = -999999999; 
	   while(values.hasNext()) {
        maxValue = Math.max(maxValue, values.next());
       }
	   output.write(key, maxValue, taskId);
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

	    Job job = new Job("Maximum",config);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setReducerNum(Integer.valueOf(config.getReducerNum()));
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
