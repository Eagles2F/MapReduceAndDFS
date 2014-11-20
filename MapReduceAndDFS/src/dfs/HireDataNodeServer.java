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
	    private int dataNodeCnt;
	    private volatile boolean running;
	    ServerSocket serverSocket;
	    
	    public HireDataNodeServer(int port,NameNode nn){
	       portNum = port;
	       this.nn = nn;
	       dataNodeCnt = 0;
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
	               System.out.println("DataNode: "+dataNodeSocket.getInetAddress()+":"+dataNodeSocket.getPort()+" joins in");
	               //nn.dataNodeSocMap.put(dataNodeCnt, dataNodeSocket); //add the dataNode soc with dataNodeCnt as the ID
	             
	               //create the specific manage server for the new worker
	               DataNodeManagerServer managerServer = new DataNodeManagerServer(nn,dataNodeCnt,dataNodeSocket); 
	               nn.dataNodeManagerMap.put(dataNodeCnt, managerServer);
	               new Thread(managerServer).start();
	               dataNodeCnt++;
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
