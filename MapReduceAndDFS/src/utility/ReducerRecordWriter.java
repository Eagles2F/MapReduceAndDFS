package utility;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.PriorityQueue;



/**
 * <code>RecordWriter</code> writes the output &lt;key, value&gt; pairs 
 * to an output file.
 
 * <p><code>RecordWriter</code> implementations write the job outputs to the
 * {@link FileSystem}.
 * 
 * @see OutputFormat
 */
public  class ReducerRecordWriter extends RecordWriter {
  /** 
   * Writes a key/value pair.
   *
   * @param key the key to write.
   * @param value the value to write.
   * @throws IOException
   */    
  

  
  @Override
  public void write(Object key, Object value, int taskId) throws IOException{
      
      String strTaskID = Long.toString(taskId);
      
      
      
      File fileToWrite = new File("Output/Intermedia/" + "reducer_" + "task" + strTaskID +".output");
      try {
          if (fileToWrite.exists() == false) {

              fileToWrite.createNewFile();

          }
         
          FileOutputStream fileStream = new FileOutputStream(fileToWrite, true);
          BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fileStream));
          
          String buf = key.toString()+" "+value.toString();
          bw.write(buf);
          bw.newLine();
          //close the writer
          bw.close();
         
          
      } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
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