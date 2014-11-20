package dfs;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/*
 * the worker.
 * @Author: Yifan Li
 * @Author: Jian Wang
 * 
 * @Date: 11/17/2014
 * @Version:0.00,developing version
 */

public class HireDataNodeServer implements Runnable{
	 	
		private NameNode nn;
	    private int portNum;//the port number which is used by this server thread
	    private volatile boolean running;
	    ServerSocket serverSocket;
	    
	    public HireDataNodeServer(int port,NameNode nn){
	       portNum = port;
	       this.nn = nn;
	       running = true;
	       try {
	        serverSocket = new ServerSocket(portNum);
	       } catch (IOException e) {
	        e.printStackTrace();
	        System.out.println("failed to create the socket server");
	        System.exit(0);
	    }
	       System.out.println("create HireDataNodeServer");
	    }
	    
	    @Override
	    public void run(){   
	       try{
	    	   System.out.println("HireDataNodeServer is waiting for new workers on the port: "+this.portNum);
	           while(running){
	               Socket dataNodeSocket = serverSocket.accept();
	               DataNodeManagerServer managerServer = new DataNodeManagerServer(nn,dataNodeSocket); 
	               new Thread(managerServer).start();
	           } 
	       }catch(IOException e){
	           e.printStackTrace();
	           System.out.println("socket server accept failed");
	       }
	       try {
	        serverSocket.close();
	       } catch (IOException e) {
	        e.printStackTrace();
	        System.out.println("socket Server failed to close");
	    }
	        
	    }
	    
	    public void stop(){
	        running = false;
	    }
	    
}
