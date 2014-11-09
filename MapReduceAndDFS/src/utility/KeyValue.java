package utility;

public class KeyValue<Key,Value>{
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
}