package dfs;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

import dfs.DFSFile.fileType;
import utility.DFSCommandId;
import utility.DFSMessage;
import utility.DFSMessage.DownloadType;
import utility.DFSMessage.nodeType;
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
		JobStatusMap = new ConcurrentHashMap<String,DFSJobStatus>();
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
    
    //generate a duplication for the dfs file and send the dup file
    public void genDup(DFSFile file){ 
    	
    	if(master.workerMangerServerMap.size() != 1){
    		//generate the dup file
    		
    		//choose the node to store the dup file
    		int dupId = (file.getNodeId()+1)%master.getHireWorkerServer().getWorkerCnt();
    		while(true){
    			if(master.workerMangerServerMap.containsKey(dupId)){
    				break;
    			}else{
    				dupId = (dupId+1)%master.getHireWorkerServer().getWorkerCnt();
    			}
    		}
    		
    		file.setDupId(dupId);
    		file.setDupLocalFilePath(file.getNodeLocalFilePath());
    		file.setDupNodeAddress(this.dataNodeSocMap.get(dupId).getInetAddress().getHostAddress());
    		
    		//send the generating file
    		
    		//generating the sending message
    		DFSMessage dfsMsg = new DFSMessage();
    		if(file.getTypeFile() == fileType.OBJECT){
    			
            	dfsMsg.setMessageType(DFSMessage.msgType.COMMAND);
            	dfsMsg.setCmdId(DFSCommandId.GETFILES);
            	dfsMsg.setDownloadType(DFSMessage.DownloadType.OBJECT);
            
            	dfsMsg.setTargetPath(file.getNodeLocalFilePath());
            	dfsMsg.setTargetFileName(file.getName());
            	dfsMsg.setLocalPath(file.getDupLocalFilePath());
            	dfsMsg.setLocalFileName(file.getDuplicationName());
            	dfsMsg.setMessageSource(nodeType.MASTER);
    		}else if(file.getTypeFile() == fileType.TXT){
            	dfsMsg.setMessageType(DFSMessage.msgType.COMMAND);
            	dfsMsg.setCmdId(DFSCommandId.GETFILES);
            	dfsMsg.setDownloadType(DFSMessage.DownloadType.TXT);
            
            	dfsMsg.setTargetPath(file.getNodeLocalFilePath());
            	dfsMsg.setTargetFileName(file.getName());
            	dfsMsg.setLocalPath(file.getDupLocalFilePath());
            	dfsMsg.setLocalFileName(file.getDuplicationName());
            	dfsMsg.setMessageSource(nodeType.MASTER);
    		}
    		
    		try {
				this.dataNodeManagerMap.get(file.getDupId()).sendToDataNode(dfsMsg);
				System.out.println();
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}
    	
    }
    
    //generate a duplication for the dfs file and send the dup file
    public void genDup(DFSFile file,Range r,Socket soc,DFSClientRequest req){ 
    	
    	if(master.workerMangerServerMap.size() != 1){
    		//generate the dup file
    		
    		//choose the node to store the dup file
    		int dupId = (file.getNodeId()+1)%master.getHireWorkerServer().getWorkerCnt();
    		while(true){
    			if(master.workerMangerServerMap.containsKey(dupId)){
    				break;
    			}else{
    				dupId = (dupId+1)%master.getHireWorkerServer().getWorkerCnt();
    			}
    		}
    		
    		file.setDupId(dupId);
    		file.setDupLocalFilePath(file.getNodeLocalFilePath());
    		file.setDupNodeAddress(this.dataNodeSocMap.get(dupId).getInetAddress().getHostAddress());
    		
    		//send the generating file
    		//tell the node to download the replication of the chunk
			DFSMessage msg1 = new DFSMessage();
			msg1.setMessageType(DFSMessage.msgType.COMMAND);
			msg1.setCmdId(DFSCommandId.GETFILES);
			msg1.setStartIndex(r.startId);
			msg1.setChunkLenth(r.endId-r.startId);
			String[] ipAddr1 = {soc.getInetAddress().getHostAddress()};
			int[] prot1 = {req.getDownloadServerPort()};
			msg1.setTargetCount(1);
			msg1.setTargetNodeAddr(ipAddr1);
			msg1.setTargetPortNum(prot1);  // set by the system configuration
			msg1.setLocalFileName(file.getDuplicationName());
			msg1.setLocalPath(file.getDuplicationName());
			msg1.setTargetPath(req.getInputFilePath());
			msg1.setTargetFileName(req.getFileName());
			msg1.setDownloadType(DownloadType.TXT);
			msg1.setMessageSource(nodeType.NAMENODE);
			msg1.setJobName(req.getJobName());
			try {
				this.dataNodeManagerMap.get(file.getDupId()).sendToDataNode(msg1);
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}
    	
    }
    //Handle dataNode failure
    public boolean handleDataNodeFailure(int NodeId){
    	return true;
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
