package utility;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.PriorityQueue;

import mapreduce.WorkerNode;



/**
 * <code>RecordWriter</code> writes the output &lt;key, value&gt; pairs 
 * to an output file.
 
 * <p><code>RecordWriter</code> implementations write the job outputs to the
 * {@link FileSystem}.
 * 
 * @see OutputFormat
 */
public class CombinerRecordWriter extends RecordWriter{
  /** 
   * Writes a key/value pair.
   *
   * @param key the key to write.
   * @param value the value to write.
   * @throws IOException
   */    
  private int reducerNum;
  private String path;
  private ArrayList<ObjectOutputStream> outputStreamArrayList;
  private WorkerNode worker;
  public CombinerRecordWriter(int redNum, String outputPath, ArrayList<ObjectOutputStream> outputStreamArray, WorkerNode worker){
      reducerNum = redNum;
      path = outputPath;
      outputStreamArrayList = outputStreamArray;
      this.worker = worker;
  }
  public void write(Object key, Object value, int taskId) throws IOException{
      
      String strTaskID = Long.toString(taskId);
      
      int fileNum = key.hashCode()%reducerNum;
      //System.out.println("combiner file num "+fileNum+"key "+key.toString());
      
          KeyValue<Object,Object> pair = new KeyValue<Object,Object>();
          pair.setKey(key);
          pair.setValue(value);
          
          ObjectOutputStream outputStream = outputStreamArrayList.get(fileNum);
          //outputStream.writeObject(pair);
          worker.writeToOutputStream(outputStream, pair);
          
          
          
          
      
  }

  /** 
   * Close this <code>RecordWriter</code> to future operations.
   * 
   * @param reporter facility to report progress.
   * @throws IOException
   */ 
  public void close() throws IOException{
      
  }

 
}
