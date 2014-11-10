package mapreduce.userlib;

import java.io.IOException;
import java.util.Iterator;



public interface Reducer<K1,V1,K2,V2>{
void reduce(K2 key, Iterator<V2> values/*,
              OutputCollector<K3, V3> output, Reporter reporter*/)
    throws IOException;
}