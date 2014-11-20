package dfs;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

import mapreduce.master.Master;

public class NameNode implements Runnable{
	
	public ConcurrentHashMap<Integer,Socket> dataNodeSocMap;
	public ConcurrentHashMap<Integer,ObjectOutputStream> dataNodeOosMap;
	public ConcurrentHashMap<Integer,DataNodeManagerServer> dataNodeManagerMap;
	public ConcurrentHashMap<String,DFSJobStatus> JobStatusMap;
	private DFSDirectory rootDir;
	private Master master;
	
	//the port this server is listening to 
	private int port;
		
	private volatile boolean running;
	ServerSocket serverSocket;
	
	public NameNode(int port, Master master){
		this.setRootDir(new DFSDirectory("~"));
		this.setPort(port);
		this.setMaster(master);
		setRunning(true);
		try {
		      serverSocket = new ServerSocket(port);
		      } catch (IOException e) {
		        e.printStackTrace();
		        System.out.println("failed to create the socket server");
		        System.exit(0);
		}
		System.out.println("create NameNode");
		dataNodeSocMap = new ConcurrentHashMap<Integer,Socket>();
		dataNodeOosMap = new ConcurrentHashMap<Integer,ObjectOutputStream>();
		dataNodeManagerMap = new ConcurrentHashMap<Integer,DataNodeManagerServer>();
	}
	public ConcurrentHashMap<Integer, DataNodeManagerServer> getDataNodeManagerMap() {
        return dataNodeManagerMap;
    }
    public void setDataNodeManagerMap(
            ConcurrentHashMap<Integer, DataNodeManagerServer> dataNodeManagerMap) {
        this.dataNodeManagerMap = dataNodeManagerMap;
    }
    @Override
	public void run() {
		   try{
	    	   System.out.println("NameNode is waiting for new requests from clients on the port: "+this.port);
	           while(running){
	               Socket dfsSocket = serverSocket.accept();
	               System.out.println("DFSClient request from "+dfsSocket.getInetAddress());
	               
					//create a new thread to handle the request
					HandleDFSClientReq handle = new HandleDFSClientReq(dfsSocket,this);
					Thread  t =new Thread(handle);
					t.start();
	           } 
	       }catch(IOException e){
	           e.printStackTrace();
	           System.out.println("socket server accept failed");
	       }
	       try {
	        serverSocket.close();
	       } catch (IOException e){ 
	        e.printStackTrace();
	        System.out.println("socket Server failed to close");
	    }		
	}
	public Master getMaster() {
		return master;
	}
	public void setMaster(Master master) {
		this.master = master;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public boolean isRunning() {
		return running;
	}
	public void setRunning(boolean running) {
		this.running = running;
	}
	public DFSDirectory getRootDir() {
		return rootDir;
	}
	public void setRootDir(DFSDirectory rootDir) {
		this.rootDir = rootDir;
	} 

}
