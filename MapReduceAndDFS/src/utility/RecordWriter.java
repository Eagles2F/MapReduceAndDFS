package utility;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
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
public abstract class RecordWriter  {
  /** 
   * Writes a key/value pair.
   *
   * @param key the key to write.
   * @param value the value to write.
   * @throws IOException
   */    
  
  public abstract void write(Object key, Object value, int taskId) throws IOException;

  /** 
   * Close this <code>RecordWriter</code> to future operations.
   * 
   * @param reporter facility to report progress.
   * @throws IOException
   */ 
  public abstract void close() throws IOException;



  
}
