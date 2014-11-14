package utility;
import java.io.File;
import java.io.IOException;
import java.util.PriorityQueue;



/**
 * <code>RecordWriter</code> writes the output &lt;key, value&gt; pairs 
 * to an output file.
 
 * <p><code>RecordWriter</code> implementations write the job outputs to the
 * {@link FileSystem}.
 * 
 * @see OutputFormat
 */
public  class MapperRecordWriter extends RecordWriter {
  /** 
   * Writes a key/value pair.
   *
   * @param key the key to write.
   * @param value the value to write.
   * @throws IOException
   */    
  PriorityQueue<KeyValue<Object, Object>> pairQ;
  public MapperRecordWriter(){
      pairQ = new PriorityQueue<KeyValue<Object, Object>>();
  }

  
  @Override
  public void write(Object key, Object value, int taskId) throws IOException{
      
      String strTaskID = Long.toString(taskId);
      
      
      
          KeyValue<Object,Object> pair = new KeyValue<Object,Object>();
          pair.setKey(key);
          pair.setValue(value);
          /*FileOutputStream fileStream = new FileOutputStream(fileToWrite, true);
          ObjectOutputStream outputStream = new ObjectOutputStream(fileStream);
          outputStream.writeObject(pair);
          
          //close the writer
          outputStream.close();*/
          pairQ.add(pair);
          
      
  }

  /** 
   * Close this <code>RecordWriter</code> to future operations.
   * 
   * @param reporter facility to report progress.
   * @throws IOException
   */ 
  public void close() throws IOException{
      
  }

  public PriorityQueue<KeyValue<Object, Object>> getPairQ(){
      return pairQ;
  }


}