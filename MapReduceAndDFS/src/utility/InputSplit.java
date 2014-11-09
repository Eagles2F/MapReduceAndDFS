package utility;

import java.io.Serializable;

public interface InputSplit extends Serializable{
    /**
     * 
     */

    //set/return the start point of the split in a file
    public void setStart(long start);
    public long getStart();
    //set/return the length in bytes of each split
    public void setLength(long length);
    
    public long getLength();
    //return the location of each split, in a array 
    public void setLocation(String fileLocation);
    
    public String getLocation();
}