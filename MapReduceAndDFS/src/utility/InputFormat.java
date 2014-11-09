package utility;
import java.io.IOException;



public interface InputFormat<K, V> {

  /** 
   * Logically split the set of input files for the job.  
   * 
   * <p>Each {@link InputSplit} is then assigned to an individual {@link Mapper}
   * for processing.</p>
   *
   * <p><i>Note</i>: The split is a <i>logical</i> split of the inputs and the
   * input files are not physically split into chunks. For e.g. a split could
   * be <i>&lt;input-file-path, start, offset&gt;</i> tuple.
   * 
   * @param job job configuration.
   * @param numSplits the desired number of splits, a hint.
   * @return an array of {@link InputSplit}s for the job.
   */
  InputSplit[] getSplits(int numSplits) throws IOException;

  /** 
   * Get the {@link RecordReader} for the given {@link InputSplit}.
   *
   * <p>It is the responsibility of the <code>RecordReader</code> to respect
   * record boundaries while processing the logical split to present a 
   * record-oriented view to the individual task.</p>
   * 
   * @param split the {@link InputSplit}
   * @param job the job that this split belongs to
   * @return a {@link RecordReader}
   */
  RecordReader<K, V> getRecordReader(InputSplit split) throws IOException;
}