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

import java.awt.TrayIcon.MessageType;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

import utility.DFSMessage;
import utility.DFSCommandId;
import utility.KeyValue;
import utility.WorkerConfig;

public class DataNode {
    private String DFSFolder;
    private WorkerConfig config;
    private int hostPort;
    private int localPort;
    private String host;
    private int recordLenth;
    
    public DataNode(int lenth){
        WorkerConfig config = new WorkerConfig();
        DFSFolder = new String("../DFS/");
        
        this.host = config.getMasterAdd();
        this.hostPort = Integer.valueOf(config.getMasterPort());
        this.localPort = Integer.valueOf(config.getLocalPort());
        recordLenth = lenth;
        createDFSDirectory();
        try {
            Socket nameNodeSocket = new Socket(host,hostPort);
            dataNodeThread t = new dataNodeThread(nameNodeSocket);
            t.start();
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
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
        }
        
    }
    
    //download server thread, it will create a new thread for every download request
    public class dataNodeDownloadServer extends Thread{


        public void run() {
            ServerSocket Listener = null;
            try {
                Listener = new ServerSocket(localPort);

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
                File downloadFile = new File(msg.getTargetPath()+"/"
                        + msg.getTargetFileName());
                if(downloadFile.exists() == false){
                    DFSMessage rspMsg = new DFSMessage();
                    rspMsg.setMessageType(DFSMessage.msgType.RESPONSE);
                    rspMsg.setResponseId(DFSMessage.rspId.DOWNLOADRSP);
                    rspMsg.setResult(DFSMessage.msgResult.FAILURE);
                    rspMsg.setCause("file not exists");
                    objectOutputStream.writeObject(rspMsg);
                    downloadSocket.close();
                }
                FileInputStream fileInput = new FileInputStream(DFSFolder
                        + msg.getTargetFileName());
                File inputFile = new File(DFSFolder
                        + msg.getTargetFileName());
                RandomAccessFile fileHdl = new RandomAccessFile(inputFile,"r");
                fileHdl.seek(msg.getRecordLenth()*msg.getStartIndex());
                
                if(msg.getDownloadType() == DFSMessage.DownloadType.OBJECT){
                    
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
                    }
                } 
                else{
                    output = downloadSocket.getOutputStream();
                    byte[] buffer = new byte[50];
                    String inputString;
                    fileHdl.readLine();
                    int readLength = -1;
                    int writeLenth = 0;
                    
                    for(int i=0;i<msg.getStartIndex()+msg.getChunkLenth();i++){
                        inputString = fileHdl.readLine();
                        if(i>=msg.getStartIndex()){
                            
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
            }
            
            
        
    }
    
    //the main thread for the dataNode, start when the system boot up
    public class dataNodeThread extends Thread {

        Socket s;

        public dataNodeThread(Socket socket) {
            this.s = socket;
        }

        public void run() {
            try {
                InputStream inputStream = s.getInputStream();
                OutputStream outputStream = s.getOutputStream();
                ObjectInputStream objInput = new ObjectInputStream(inputStream);

                
                ObjectOutputStream objOutput = new ObjectOutputStream(outputStream);
                
                /* read a message from the other end */
                DFSMessage msg = (DFSMessage) objInput.readObject();
                System.out.println("receive message: "+msg.getCmdId());
                if(msg.getCmdId() == DFSCommandId.GETFILES){
                    DFSMessage rspMsg = downloadFiles(msg);
                    objOutput.writeObject(rspMsg);
                }
                    
                    
                }catch(Exception e){
                    
                }
                
        }
        }

    /*download files from other dataNode
     * nameNode will send the getFiles request to the target dataNode
     * source dataNode will download the file from the source dataNode
     */
    public DFSMessage downloadFiles(DFSMessage msg) {
        System.out.println("Start File Transfer from " + msg.getTargetNodeAddr() + " "
                + msg.getTargetPortNum());
        DFSMessage rspMsg = new DFSMessage();
        rspMsg.setMessageType(DFSMessage.msgType.RESPONSE);
        rspMsg.setResponseId(DFSMessage.rspId.GETFILESRSP);
        Socket socket = null;
        try {
            socket = new Socket(msg.getTargetNodeAddr(), msg.getTargetPortNum());
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
        DFSMessage downloadMsg = new DFSMessage();
        downloadMsg.setMessageType(DFSMessage.msgType.COMMAND);
        downloadMsg.setCmdId(DFSCommandId.DOWNLOAD);
        downloadMsg.setTargetFileName(msg.getTargetFileName());
        downloadMsg.setTargetPath(msg.getTargetPath());
        downloadMsg.setStartIndex(msg.getStartIndex());
        downloadMsg.setChunkLenth(msg.getChunkLenth());
        
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
            try {
                socket.close();
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            return rspMsg;
        }
        try {
            DFSMessage downloadMsgRsp = (DFSMessage)objInput.readObject();
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
            
            /*
             * create the file and write what the server get from socket into the
             * file
             */
            FileOutputStream fileOutput = null;
            try {
                File outputFile = new File(msg.getLocalPath() + "/" 
                        + msg.getLocalFileName());
                if(!outputFile.exists()){
                    outputFile.createNewFile();
                }
                fileOutput = new FileOutputStream(msg.getLocalPath() + "/"
                        + msg.getLocalFileName(),true);
                byte[] buffer = new byte[50];
                int length = -1;
                try {
                    while ((length = input.read(buffer)) > 0) {
                        
                        fileOutput.write(buffer, 0, length);
                        fileOutput.flush();
                    }
                    fileOutput.close();
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
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            rspMsg.setResult(DFSMessage.msgResult.FAILURE);
            rspMsg.setCause("IOException");
            return rspMsg;
        }

        
        

        System.out.println("Finish File Transfer");
        try {
            socket.close();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        return rspMsg;
        
        
    }
    

}
