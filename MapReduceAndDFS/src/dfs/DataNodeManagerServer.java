package dfs;


import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import mapreduce.Task;
import mapreduce.TaskStatus;
import mapreduce.TaskStatus.taskState;
import mapreduce.WorkerNodeStatus;
import utility.ClientMessage;
import utility.CommandType;
import utility.DFSMessage;
import utility.Message;
import utility.Message.msgResult;
import utility.Message.msgType;


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

    public DataNodeManagerServer(NameNode nn, int id, Socket s) throws IOException {

        this.nn = nn;
        dataNodeId = id;
        running = true;
        System.out.println("adding a new manager server for worker "+id);
        socket = nn.dataNodeSocMap.get(id);
        objInput = new ObjectInputStream(socket.getInputStream());
        objOutput = new ObjectOutputStream(socket.getOutputStream());
        nn.dataNodeOosMap.put(id, objOutput);//add the OOS to the map
     
    }

    public void run(){
        try{
        	
            DFSMessage msg;
            System.out.println("managerServer for dataNode "+dataNodeId+" running");
            
            //start to listen to the response from the dataNode
            while(running){
                
            	//receive the msg
                try{
                    msg = (DFSMessage) objInput.readObject();
                    System.out.println("receive "+msg.getMessageType());
                }catch(ClassNotFoundException e){
                    continue;
                }   
                
                //process the msg
           
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
                    		System.out.println("unrecagnized message");
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
    public int sendToDataNode(Message cmd) throws IOException{
        objOutput.writeObject(cmd);
        objOutput.flush();
        return 0;
}

    
    public void stop(){
        running = false;
    }
}
