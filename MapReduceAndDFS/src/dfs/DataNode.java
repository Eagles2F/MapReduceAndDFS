/*
 * This is the dataNode for DFS. the dataNode will run on the same machine with the MapReduce worker node
 * every dataNode will have a thread the receive the message from nameNode and will have a download server thread
 * to receive the downlaod request from other dataNode
 * @Author: Yifan Li
 * @Author: Jian Wang
 * 
 * @Date: 11/9/2014
 * @Version:0.00,developing version
 */
package dfs;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.Charset;

import mapreduce.WorkerNode;
import utility.DFSMessage;
import utility.DFSCommandId;
import utility.DFSMessage.msgResult;
import utility.IndicationType;
import utility.KeyValue;
import utility.Message;
import utility.Message.msgType;
import utility.WorkerConfig;

public class DataNode implements Runnable{
    private String DFSFolder;
    
    private int hostPort;
    private int localPort;
    private String host;
    
    private WorkerNode worker;
    private boolean exit;
    private ObjectOutputStream objOutput;
    private ObjectInputStream objInput;

    private String tempDfsDir;

    private String inputChunkDir;
    
    
    public DataNode( WorkerNode worker) {
        this.worker = worker;
        WorkerConfig config = new WorkerConfig();
        DFSFolder = new String("../DFS/");
        this.tempDfsDir = new String("../DFS/temp/");
        this.inputChunkDir = new String("../DFS/InputChunk/");
        
        this.host = config.getMasterAdd();
        this.hostPort = Integer.valueOf(config.getDataNodeServerPort());
        this.localPort = Integer.valueOf(config.getLocalPort());

        createDFSDirectory();
        
            
            dataNodeDownloadServer t = new dataNodeDownloadServer();
            t.start();
            System.out.println("starting download server");
            //create the socket with the managerServer
            Socket s = null;
            System.out.println("dataNode Thread running");
            try {
                s = new Socket(host,hostPort);
                System.out.println("socket to nameNode created");
                OutputStream outputStream = s.getOutputStream();
                InputStream inputStream = s.getInputStream();
                
                System.out.println("create object stream");
                objOutput = new ObjectOutputStream(outputStream);
                objInput = new ObjectInputStream(inputStream);

                System.out.println("createed input object stream");
            } catch (UnknownHostException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
                System.out.println("dataNode UnknownHostException");
                
                return;
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
                
                System.out.println("dataNode IOException");
                return;
            }
            System.out.println("create stream");
            
        
    }

    private void createDFSDirectory() {
        File folder = new File(DFSFolder);
        /* create working directory */
        if (!folder.exists()) {
            if (folder.mkdir()) {
                System.out.println("DFS Directory is created!");
            } else {
                System.err.println("Failed to create DFS directory!");
            }
        }
        /* delete all files in the directory */
        else {
            File[] listOfFiles = folder.listFiles();
            for (File file : listOfFiles)
                file.delete();
            File f = null;  
            
            f = new File(tempDfsDir);  
            if(f.exists()){
                System.out.println("delete temp directory");
                File[] files = f.listFiles(); // 得到f文件夹下面的所有文件。  
                
                for (File file : files) {  
                    file.delete(); 
                }  
            }
            
            f = new File(inputChunkDir); 
            if(f.exists()){
                System.out.println("delete chunk directory");
                File[] chunkFiles = f.listFiles(); // 得到f文件夹下面的所有文件。  
                
                for (File file : chunkFiles) {  
                    file.delete(); 
                } 
            }
        }
        
    }
    
    public boolean isExit() {
        return exit;
    }

    public void setExit(boolean exit) {
        this.exit = exit;
    }

    //download server thread, it will create a new thread for every download request
    public class dataNodeDownloadServer extends Thread{


        public void run() {
            ServerSocket Listener = null;
            try {
                Listener = new ServerSocket(localPort);
                System.out.println("download server stated");
                while (true) {
                    //waiting for the download request from other data node
                    Socket downloadSocket = Listener.accept();

                    System.out.println("Socket accepted from " + downloadSocket.getInetAddress()
                            + " " + downloadSocket.getPort());
                    dataNodeDownloadThread downloadThread = new dataNodeDownloadThread(downloadSocket);
                    downloadThread.start();
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    
    }
    
    //the acutual working thread for any download request
	  public class dataNodeDownloadThread extends Thread{
	        private Socket downloadSocket;

	        public dataNodeDownloadThread(Socket s){
	            downloadSocket = s;

	        }
	        public void run(){
	            OutputStream output;
	            InputStream input;
	            try {
	                output = downloadSocket.getOutputStream();
	                ObjectOutputStream objectOutputStream = new ObjectOutputStream(output);
	                input = downloadSocket.getInputStream();
	                ObjectInputStream objectInputStream = new ObjectInputStream(input);
	                DFSMessage msg = null;
	                try {
	                    msg = (DFSMessage) objectInputStream.readObject();
	                } catch (ClassNotFoundException e1) {
	                    // TODO Auto-generated catch block
	                    e1.printStackTrace();
	                }
	                /*
	                 * create the file and write what the server get from socket into the
	                 * file
	                 */
	                System.out.println("download server: request  file "+msg.getTargetPath()+"/"
	                        + msg.getTargetFileName());
	                File downloadFile = new File(msg.getTargetPath()+"/"
	                        + msg.getTargetFileName());
	                
	                //send dfs download msg
	                DFSMessage rspMsg = new DFSMessage();
	                rspMsg.setMessageType(DFSMessage.msgType.RESPONSE);
	                rspMsg.setResponseId(DFSMessage.rspId.DOWNLOADRSP);
	                rspMsg.setResult(DFSMessage.msgResult.SUCCESS);
	                if(downloadFile.exists() == false){
	                    rspMsg.setResult(DFSMessage.msgResult.FAILURE);
	                    rspMsg.setCause("file "+msg.getTargetPath()+"/"
	                            + msg.getTargetFileName()+"not exists");
	                    objectOutputStream.writeObject(rspMsg);
	                    
	                }
	                
	                objectOutputStream.writeObject(rspMsg);
	                FileInputStream fileInput = new FileInputStream(msg.getTargetPath()+"/"
	                        + msg.getTargetFileName());
	                     
	                if(msg.getDownloadType() == DFSMessage.DownloadType.OBJECT){
	                    System.out.println("download Server:Object file downloading ");
	                    ObjectInputStream inputStream = new ObjectInputStream(fileInput);
	                    KeyValue<Object,Object> pair = null;
	                    try {
	                        while ((pair = (KeyValue<Object, Object>) inputStream.readObject()) != null) {
	                            objectOutputStream.writeObject(pair);
	                            objectOutputStream.flush();
	                        }
	                    } catch (ClassNotFoundException e) {
	                        // TODO Auto-generated catch block
	                        e.printStackTrace();
	                        downloadSocket.close();
	                    }catch(EOFException e){
	                        //reach file end, do nothing
	                        System.out.println("download file end");
	                    }
	                    System.out.println("server:download finish");
	                } 
	                else{
	                    output = downloadSocket.getOutputStream();
	                    byte[] buffer ;
	                    String inputString;
	                    BufferedReader br = new BufferedReader(new InputStreamReader(fileInput,Charset.forName("UTF-8")));
	                    //read the whole file
	                    if(msg.getChunkLenth() == -1){

                            while((inputString = br.readLine()) != null){
                            
                                
                                inputString += '\n';
                                buffer = inputString.getBytes();
                                
                                if(inputString.length() <= 50)
                                    output.write(buffer, 0, inputString.length());
                                else{
                                    int j=0;
                                    for(j =0 ;j<(inputString.length()/50);j++){
                                        output.write(buffer, j*50, 50);
                                    }
                                    //the remaining part
                                    output.write(buffer, j*50, inputString.length()-j*50);
                                }
                            }
                            
                        
	                        
	                    }else{
	                    for(int i=0;i<msg.getStartIndex()+msg.getChunkLenth();i++){
	                        inputString = br.readLine();
	                        if(i>=msg.getStartIndex()){
	                            inputString += '\n';
	                            buffer = inputString.getBytes();
	                            
	                            if(inputString.length() <= 50)
	                                output.write(buffer, 0, inputString.length());
	                            else{
	                                int j=0;
	                                for(j =0 ;j<(inputString.length()/50);j++){
	                                    output.write(buffer, j*50, 50);
	                                }
	                                //the remaining part
	                                output.write(buffer, j*50, inputString.length()-j*50);
	                            }
	                        }
	                        
	                    }
	                  }
	                }
	            }catch (IOException e) {
	                    // TODO Auto-generated catch block
	                    e.printStackTrace();
	                    try {
	                        downloadSocket.close();
	                    } catch (IOException e1) {
	                        // TODO Auto-generated catch block
	                        e1.printStackTrace();
	                    }
	                }
	            try {
	                downloadSocket.close();
	            } catch (IOException e) {
	                // TODO Auto-generated catch block
	                e.printStackTrace();
	            }
	            
	            }
      }
    
    //the main thread for the dataNode, start when the system boot up
    

        public void run() {
            
            try {
                
                
                
                //send the joinIn indication to nameNode with the workerId get from master
                System.out.println("send join in to NameNode");
                DFSMessage joinIn = new DFSMessage();
                joinIn.setMessageType(DFSMessage.msgType.INDICATION);
                joinIn.setIndicationId(DFSMessage.indId.JOININ);
                joinIn.setWorkerID(worker.getWorkerID());
                objOutput.writeObject(joinIn);
                
                /* read a message from the other end */
                DFSMessage msg = null;
                while(!exit){
                    System.out.println("wait for dataNode request");
                    msg = (DFSMessage) objInput.readObject();
                    System.out.println("receive message: "+msg.getCmdId()+" task "+msg.getTaskId());
                    if(msg.getCmdId() == DFSCommandId.GETFILES){
                        downloadFiles(msg);
                        //objOutput.writeObject(rspMsg);
                    }else if(msg.getCmdId() == DFSCommandId.RENAME){
                        File oldFile = new File(msg.getLocalPath()+"/"+msg.getLocalFileName());
                        File newFile = new File(msg.getLocalPath()+"/"+msg.getTargetFileName());
                        oldFile.renameTo(newFile);
                    }
                    
                }   
                }catch(Exception e){
                    
                }
               
        }
        

    /*download files from other dataNode
     * nameNode will send the getFiles request to the target dataNode
     * source dataNode will download the file from the source dataNode
     */
    public DFSMessage downloadFiles(DFSMessage msg) {
        System.out.println("Start File Transfer" );
        DFSMessage rspMsg = new DFSMessage();
        rspMsg.setMessageType(DFSMessage.msgType.RESPONSE);
        rspMsg.setResponseId(DFSMessage.rspId.GETFILESRSP);
        Socket socket = null; //socket with the target dataNode to download
        File outputFile = new File(msg.getLocalPath() + "/" 
                + msg.getLocalFileName());
        System.out.println("write to "+msg.getLocalPath() + "/" 
                + msg.getLocalFileName());
        File outputDir = new File(msg.getLocalPath());
        if(!outputDir.exists()){
            System.out.println("create path");
            outputDir.mkdir();
            
        }
        if(!outputFile.exists()){
            try {
                outputFile.createNewFile();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        FileOutputStream fileOutput = null;
        System.out.println("write to path"+msg.getLocalPath());
        
        try {
            fileOutput = new FileOutputStream(msg.getLocalPath() + "/"
                    + msg.getLocalFileName(),true);
        } catch (FileNotFoundException e2) {
            // TODO Auto-generated catch block
            e2.printStackTrace();
        }
        ObjectOutputStream objectOutput = null;
        System.out.println("download "+msg.getTargetCount()+" files");
        for(int k=0;k<msg.getTargetCount();k++){
            System.out.println("Start File"+k+" Transfer from " + msg.getTargetNodeAddr()[k] + " "
                    + msg.getTargetPortNum()[k]);
            try {
                //create socket with target node
                socket = new Socket(msg.getTargetNodeAddr()[k], msg.getTargetPortNum()[k]);
            } catch (UnknownHostException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                rspMsg.setResult(DFSMessage.msgResult.FAILURE);
                rspMsg.setCause("UnknownHostException");
                
                return rspMsg;
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                rspMsg.setResult(DFSMessage.msgResult.FAILURE);
                rspMsg.setCause("IOException");
                return rspMsg;
            }
    
            // send the file name and range
            OutputStream msgOutput = null;
            InputStream input = null;
            try {
                msgOutput = socket.getOutputStream();
                input = socket.getInputStream();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                rspMsg.setResult(DFSMessage.msgResult.FAILURE);
                rspMsg.setCause("IOException");
                try {
                    socket.close();
                } catch (IOException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                return rspMsg;
            }
            ObjectOutputStream objOutput = null;
            ObjectInputStream objInput = null;
            try {
                System.out.println("setup the output input stream with download server");
                objOutput = new ObjectOutputStream(msgOutput);
                objInput = new ObjectInputStream(input);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                try {
                    socket.close();
                } catch (IOException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                rspMsg.setResult(DFSMessage.msgResult.FAILURE);
                rspMsg.setCause("IOException");
                return rspMsg;
            }
            System.out.println("send download message to target node");
            DFSMessage downloadMsg = new DFSMessage();
            downloadMsg.setMessageType(DFSMessage.msgType.COMMAND);
            downloadMsg.setCmdId(DFSCommandId.DOWNLOAD);
            downloadMsg.setTargetFileName(msg.getTargetFileName());
            downloadMsg.setTargetPath(msg.getTargetPath());
            downloadMsg.setStartIndex(msg.getStartIndex());
            downloadMsg.setChunkLenth(msg.getChunkLenth());
            downloadMsg.setDownloadType(msg.getDownloadType());
            
            try {
                objOutput.writeObject(downloadMsg);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                try {
                    objOutput.close();
                } catch (IOException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                try {
                    objInput.close();
                } catch (IOException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                rspMsg.setResult(DFSMessage.msgResult.FAILURE);
                rspMsg.setCause("IOException");
                
            }
            try {
                DFSMessage downloadMsgRsp = (DFSMessage)objInput.readObject();
                System.out.println("receive download rsp "+downloadMsgRsp.getResult()+" type "+downloadMsgRsp.getDownloadType());
                if(downloadMsgRsp.getResult() != DFSMessage.msgResult.SUCCESS){
                    System.out.println("download file "+msg.getTargetFileName()+" failed");
                    rspMsg.setResult(DFSMessage.msgResult.FAILURE);
                    rspMsg.setCause(downloadMsgRsp.getCause());
                    try {
                        socket.close();
                    } catch (IOException e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                    return rspMsg;
                }
            } catch (ClassNotFoundException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
                try {
                    objOutput.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                try {
                    objInput.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                rspMsg.setResult(DFSMessage.msgResult.FAILURE);
                rspMsg.setCause("ClassNotFoundException");
                try {
                    socket.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                return rspMsg;
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
                try {
                    objOutput.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                try {
                    objInput.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                rspMsg.setResult(DFSMessage.msgResult.FAILURE);
                rspMsg.setCause("IOException");
                try {
                    socket.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                return rspMsg;
            }
            
            
            
            try {
                
                
                
                if(msg.getDownloadType() == DFSMessage.DownloadType.OBJECT){
                    //ObjectInputStream objectInput = new ObjectInputStream(input);
                    if(objectOutput == null){
                        try {
                            objectOutput = new ObjectOutputStream(fileOutput);
                        } catch (IOException e2) {
                            // TODO Auto-generated catch block
                            e2.printStackTrace();
                        }
                    }
                    
                    System.out.println("start receiving object file");
                    KeyValue<Object,Object> pair = null;
                    try {
                        while( (pair = (KeyValue<Object, Object>) objInput.readObject()) != null){
                            System.out.println("key "+pair.getKey()+" value"+pair.getValue());
                            objectOutput.writeObject(pair);
                        }
                        
                        
                    } catch (ClassNotFoundException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        
                    }catch(EOFException e){
                        //reach file end, do nothing
                        System.out.println("receiving file finish");
                    }
                    System.out.println("download finished");
                    
                }
                else{
                /*
                 * create the file and write what the server get from socket into the
                 * file
                 */
                
                try {
                    
                    byte[] buffer = new byte[50];
                    int length = -1;
                    try {
                        while ((length = input.read(buffer)) > 0) {
                            
                            fileOutput.write(buffer, 0, length);
                            fileOutput.flush();
                        }
                        
                        rspMsg.setResult(msgResult.SUCCESS);
                        System.out.println("download finished");
                        
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        rspMsg.setResult(DFSMessage.msgResult.FAILURE);
                        rspMsg.setCause("IOException");
                        fileOutput.close();
                        return rspMsg;
                    }
                } catch (FileNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    rspMsg.setResult(DFSMessage.msgResult.FAILURE);
                    rspMsg.setCause("FileNotFoundException");
                    return rspMsg;
                }
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                rspMsg.setResult(DFSMessage.msgResult.FAILURE);
                rspMsg.setCause("IOException");
                return rspMsg;
            }
        
        }//end of for loop
        System.out.println("Finish File Transfer");
        /*try {
            socket.close();
            objectOutput.close();
            
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }*/
      //if master send this download message, we need send complete indication
        //to master
        if(msg.getMessageSource() == DFSMessage.nodeType.MASTER){
            System.out.println("send get file complete to master");
            Message indMsg = new Message();
            indMsg.setMessageType(msgType.INDICATION);
            indMsg.setIndicationId(IndicationType.GETFILESCOMPLETE);
            indMsg.setJobId(msg.getJobId());
            indMsg.setTaskId(msg.getTaskId());
            worker.sendToManager(indMsg);
            
        }
      
        
        return rspMsg;    
    }

    synchronized private void sendToDataNodeManager(DFSMessage indMsg) {
        try {
            System.out.println("send "+indMsg.getMessageType()+" "+indMsg.getResponseId()+" "+indMsg.getIndicationId());
            objOutput.writeObject(indMsg);
        } catch (IOException e) {
            
            System.err.println("fail to send dataNode manager");
        }
        
    }
    

}
