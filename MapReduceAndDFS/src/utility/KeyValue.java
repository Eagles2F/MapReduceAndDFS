package utility;

public class KeyValue<Key,Value> implements Comparable{
    private Key key;
    private Value value;
    
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
        compareHash = o.hashCode();
            
        return thisHash - compareHash;
    }
}