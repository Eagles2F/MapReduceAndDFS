package utility;

import java.io.Serializable;

public class KeyValue<Key,Value> implements Comparable,Serializable{
    private Key key;
    private Value value;
    
    public KeyValue(){
    	
    }
    public KeyValue(Key k, Value v){
    	this.key = k;
    	this.value = v;
    }
    
    public void setKey(Key k){
        key = k;
    }
    
    public Key getKey(){
        return key;
    }
    
    public void setValue(Value v){
        value = v;
    }
    
    public Value getValue(){
        return value;
    }

    @Override
    public int compareTo(Object o) {
        int thisHash;
        int compareHash;
        thisHash = key.hashCode();
        compareHash = ((KeyValue<Object,Object>)o).getKey().hashCode();
            
        return thisHash - compareHash;
    }
}