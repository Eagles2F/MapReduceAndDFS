package dfs;


import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import utility.DFSMessage;


/*
 * This is the specific manager server for each dataNode. This server will communicate with the dataNode and handle all
 * the response and state update messages from the dataNode.
 * 
 * @Author: Yifan Li
 *
 * @Date: 11/17/2014
 * @Version:0.00,developing version
 */
public class DataNodeManagerServer implements Runnable{
    private NameNode nn;
    private int dataNodeId;
    private Socket socket;
    private volatile boolean running;

    private ObjectInputStream objInput;
    private ObjectOutputStream objOutput;

    public DataNodeManagerServer(NameNode nn, Socket s) throws IOException {

        this.nn = nn;
        running = true; 
        socket = s;
        objOutput = new ObjectOutputStream(socket.getOutputStream());
        objInput = new ObjectInputStream(socket.getInputStream());
     
    }
    
    
    private void handleGetfileComplete(DFSMessage msg){
    	//set the upload task to be completed
    	System.out.println("DataNode: "+msg.getWorkerID()+"has received the inputfile chunk for job:"+msg.getJobName());
    	this.nn.JobStatusMap.get(msg.getJobName()).getUploadStatusMap().remove(msg.getWorkerID());
    	this.nn.JobStatusMap.get(msg.getJobName()).getUploadStatusMap().put(msg.getWorkerID(),true);
    }
    
    public void run(){
        try{
        	
            DFSMessage msg = null;
            System.out.println("managerServer for dataNode "+dataNodeId+" running");
            
            //start to listen to the response from the dataNode
            while(running){
                
            	//receive the msg
                try{
                    msg = (DFSMessage) objInput.readObject();
                    System.out.println("receive "+msg.getMessageType());
                }catch(ClassNotFoundException e){
                    continue;
                }catch(EOFException e){
                    //reach file end, do nothing
                    continue;
                }   
                
                //process the msg
                if(msg.getMessageType() == DFSMessage.msgType.INDICATION){
                    switch(msg.getIndicationId()) {
                    case JOININ:
                        System.out.println("DFS dataNode "+msg.getWorkerID()+" join in");
                        nn.getDataNodeManagerMap().put(msg.getWorkerID(), this);
                        this.dataNodeId = msg.getWorkerID();
                        nn.dataNodeSocMap.put(dataNodeId, socket);
                        nn.dataNodeOosMap.put(dataNodeId, objOutput);//add the OOS to the map
                        break;
                    case GETFILESCOMPLETE:
                    	handleGetfileComplete(msg);
                    default:
                        System.out.println("unrecagnized DFS indication");
                    }
                }
           
                if(msg.getMessageType() == DFSMessage.msgType.RESPONSE){
                	switch(msg.getResponseId()){
                		case CREATERSP:
                			break;
                		case REMOVERSP:
                			break;
                		case GETFILESRSP:
                			break;
                		case DOWNLOADRSP:
                			break;
                    	default:
                    		System.out.println("unrecagnized DFS message");
                	}
                }
            }
            objInput.close();
            objOutput.close();
        }catch(IOException e){
          e.printStackTrace();  
        }
    }
    // the send msg method
    public int sendToDataNode(DFSMessage msg) throws IOException{
        objOutput.writeObject(msg);
        objOutput.flush();
        return 0;
}

    
    public void stop(){
        running = false;
    }
}
