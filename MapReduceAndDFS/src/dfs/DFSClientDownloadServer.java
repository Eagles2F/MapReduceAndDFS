package dfs;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;

import utility.DFSMessage;
import utility.KeyValue;

public class DFSClientDownloadServer implements Runnable {
	private DFSClient client;
	
	public DFSClientDownloadServer(DFSClient client){
		this.client = client;
		System.out.println("Start the download server!");
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
	                DFSMessage rspMsg = new DFSMessage();
	                rspMsg.setMessageType(DFSMessage.msgType.RESPONSE);
	                rspMsg.setResponseId(DFSMessage.rspId.DOWNLOADRSP);
	                rspMsg.setResult(DFSMessage.msgResult.SUCCESS);
	                if(downloadFile.exists() == false){
	                    rspMsg.setResult(DFSMessage.msgResult.FAILURE);
	                    rspMsg.setCause("file "+msg.getTargetPath()+"/"
	                            + msg.getTargetFileName()+"not exists");
	                    objectOutputStream.writeObject(rspMsg);
	                    downloadSocket.close();
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
}

