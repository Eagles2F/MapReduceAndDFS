package mapreduce.userlib;

import java.io.IOException;
import java.util.Iterator;

import utility.RecordWriter;



public interface Reducer<K1,V1,K2,V2>{
void reduce(K2 key, Iterator<V2> values,
        RecordWriter outPut,int taskId)
    throws IOException;
}