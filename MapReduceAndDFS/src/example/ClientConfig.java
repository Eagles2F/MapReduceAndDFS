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
        System.out.println("master add"+prop.getProperty("MasterAddress"));
        return prop.getProperty("MasterAddress");
        
    }
    public String getMasterPort(){
        System.out.println("master port"+prop.getProperty("MasterPort"));
        return prop.getProperty("MasterPort");
    }
    
    

    @Override
    public Properties getProperties() {
        return prop;
    }
    
}