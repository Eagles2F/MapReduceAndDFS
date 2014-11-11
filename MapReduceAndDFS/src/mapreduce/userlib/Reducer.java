package mapreduce.userlib;

import java.io.IOException;
import utility.RecordWriter;



public interface Reducer<K1,V1,K2,V2>{
void reduce(K2 key, Iterable<V2> values,
        RecordWriter<K2, V2> outPut,int taskId)
    throws IOException;
}