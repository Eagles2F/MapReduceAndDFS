package example;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import utility.ConfigurationBase;

public class ClientConfig extends ConfigurationBase implements Serializable{
    
    public ClientConfig(){
        
        prop = new Properties();
        try {
            input = new FileInputStream("../clientConfig.properties");
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        try {
            prop.load(input);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        
    }
    
    public String getMasterAdd(){
        
        return prop.getProperty("MasterAddress");
        
    }
    public String getMasterPort(){
        
        return prop.getProperty("MasterPort");
    }
    
    public String getRecordNum(){
        return prop.getProperty("RecordNum");
    }

    @Override
    public Properties getProperties() {
        return prop;
    }
    
}