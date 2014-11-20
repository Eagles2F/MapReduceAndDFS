package dfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;

import utility.DFSMessage;
import utility.KeyValue;

public class DFSClientDownloadServer implements Runnable {
	private DFSClient client;
	
	public DFSClientDownloadServer(DFSClient client){
		this.client = client;
	}
	
	public void run() {
         ServerSocket Listener = null;
         try {
             Listener = new ServerSocket(client.getDownloadServerPort());

             while (true) {
                 //waiting for the download request from other data node
                 Socket downloadSocket = Listener.accept();
                 System.out.println("Socket accepted from " + downloadSocket.getInetAddress()
                         + " " + downloadSocket.getPort());
                 
                 dataNodeDownloadThread downloadThread = new dataNodeDownloadThread(downloadSocket);
                 downloadThread.start();
             }
         } catch (IOException e) {
             e.printStackTrace();
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
	                FileInputStream fileInput = new FileInputStream(msg.getTargetPath()
	                        + msg.getTargetFileName());
	                File inputFile = new File(msg.getTargetPath()
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
}

