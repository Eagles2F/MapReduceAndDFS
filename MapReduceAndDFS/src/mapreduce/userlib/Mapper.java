package mapreduce.userlib;

import java.io.IOException;

import utility.RecordWriter;

public interface Mapper<K1,V1,K2,V2>{
    void map(K1 key, V1 value, RecordWriter<K2, V2> outPut)
            throws IOException;
          
}