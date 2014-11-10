package mapreduce;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;


/*
 * This is the a runnable class which is created by the master to listen to the new connection requests from the
 * workers. After receiving the request, this server will created a special server responsible to communicate with
 * the worker.
 * @Author: Yifan Li
 * @Author: Jian Wang
 * 
 * @Date: 11/9/2014
 * @Version:0.00,developing version
 */

public class HireWorkerServer implements Runnable{
	 	
		private Master master;
	    private int portNum;//the port number which is used by this server thread
	    private int workerCnt;
	    private volatile boolean running;
	    ServerSocket serverSocket;
	    
	    public HireWorkerServer(int port,Master master){
	       portNum = port;
	       this.master = master;
	       workerCnt = 0;
	       running = true;
	       try {
	        serverSocket = new ServerSocket(portNum);
	    } catch (IOException e) {
	        e.printStackTrace();
	        System.out.println("failed to create the socket server");
	        System.exit(0);
	    }
	       System.out.println("create HireWorkerServer");
	    }
	    
	    @Override
	    public void run(){   
	       try{
	    	   System.out.println("waiting for new workers");
	           while(running){
	               Socket workerSocket = serverSocket.accept();
	               System.out.println("worker: "+workerSocket.getInetAddress()+":"+workerSocket.getPort()+" join in");
	               master.workerSocMap.put(workerCnt, workerSocket); //add the worker soc with workerCnt as the ID
	               
	               //create the specific manage server for the new worker
	               WorkerManagerServer managerServer = new WorkerManagerServer(master,workerCnt,workerSocket); 
	               master.workerMangerServerMap.put(workerCnt, managerServer);
	               new Thread(managerServer).start();
	               workerCnt++;
	           } 
	       }catch(IOException e){
	           e.printStackTrace();
	           System.out.println("socket server accept failed");
	       }
	       try {
	        serverSocket.close();
	       } catch (IOException e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
	        System.out.println("socket Server failed to close");
	    }
	        
	    }
	    
	    public void stop(){
	        running = false;
	    }
	    
}
