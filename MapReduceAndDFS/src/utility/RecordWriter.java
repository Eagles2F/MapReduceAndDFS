package utility;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;



/**
 * <code>RecordWriter</code> writes the output &lt;key, value&gt; pairs 
 * to an output file.
 
 * <p><code>RecordWriter</code> implementations write the job outputs to the
 * {@link FileSystem}.
 * 
 * @see OutputFormat
 */
public class RecordWriter<K, V> {
  /** 
   * Writes a key/value pair.
   *
   * @param key the key to write.
   * @param value the value to write.
   * @throws IOException
   */      
  void write(K key, V value) throws IOException{
      /*
      String strTaskID = Long.toString(taskID);
      File fileToWrite = new File("MapReduce/Intermedia/" + strTaskID +".out");
      try {
          if (fileToWrite.exists() == false) {

              fileToWrite.createNewFile();

          }
          FileOutputStream fileStream = new FileOutputStream(fileToWrite, true);
          ObjectOutputStream outputStream = new ObjectOutputStream(fileStream);
          outputStream.writeObject(keyValuePair);
          
          //close the writer
          outputStream.close();
          
      } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }*/
  }

  /** 
   * Close this <code>RecordWriter</code> to future operations.
   * 
   * @param reporter facility to report progress.
   * @throws IOException
   */ 
  void close() throws IOException{
      
  }
}
