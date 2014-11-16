package dfs;

import java.io.BufferedReader;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import dfs.DFSClientRequest.RequestType;

/*
 * This class is the DFS client which is running on the user side to upload the file to the DFS.
 */
public class DFSClient {
	private String DFSNameNodeIpAddr;
	private int DFSNameNodePort;
	private Socket soc;
	private BufferedReader console;
	private boolean running;
	
	private ObjectOutputStream oos;
	private ObjectInputStream ois;
	
	public DFSClient(String ip, int port){
		this.setDFSNameNodeIpAddr(ip);
		this.setDFSNameNodePort(port);
		try {
			this.soc = new Socket(ip,port);
			oos =new ObjectOutputStream(this.soc.getOutputStream());
			ois =new ObjectInputStream(this.soc.getInputStream());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public  static void main(String[] args) throws IOException{
		if(args.length != 2){
			System.out.println("The usage should be: java dfs.DFSClient <NameNode IP> <NameNode port>");
		}
		
		DFSClient dfsNN = new DFSClient(args[0],Integer.valueOf(args[1]));
		
		dfsNN.startConsole();
		
	}

	public String getDFSNameNodeIpAddr() {
		return DFSNameNodeIpAddr;
	}

	public void setDFSNameNodeIpAddr(String dFSNameNodeIpAddr) {
		DFSNameNodeIpAddr = dFSNameNodeIpAddr;
	}

	public int getDFSNameNodePort() {
		return DFSNameNodePort;
	}

	public void setDFSNameNodePort(int dFSNameNodePort) {
		DFSNameNodePort = dFSNameNodePort;
	}
	
	//utility method
	public int countLines(String filename) throws IOException {
	    InputStream is = new BufferedInputStream(new FileInputStream(filename));
	    try {
	        byte[] c = new byte[1024];
	        int count = 0;
	        int readChars = 0;
	        boolean empty = true;
	        while ((readChars = is.read(c)) != -1) {
	            empty = false;
	            for (int i = 0; i < readChars; ++i) {
	                if (c[i] == '\n') {
	                    ++count;
	                }
	            }
	        }
	        return (count == 0 && !empty) ? 1 : count;
	    } finally {
	        is.close();
	    }
	}
	
	
	//cmd handle method
	public void handleUp(String[] cmd) throws IOException{
		if(cmd.length != 4){
			System.out.println("Usage:upload <local input file path> <destination directory on DFS> <JobClassName>");
			return ;
		}
		
		DFSClientRequest req = new DFSClientRequest(RequestType.InputUpload);
		req.setInputFilePath(cmd[1]);
		req.setDesDFSDir(cmd[2]);
		req.setJobName(cmd[3]);
		req.setFileLineNum(countLines(cmd[1]));//set the line number of the file
		
		String[] temp = cmd[1].split("/");
		req.setFileName(temp[temp.length-1]);
		sendToNN(req);
		System.out.println("Send upload request");
	}
	public void startConsole() throws IOException{
        System.out.println("This is DFS client, type help for more information");
        
        String cmdLine=null;
        while(running){
            System.out.print(">>");
            try{
                cmdLine = console.readLine();
                
            }catch(IOException e){
                System.out.println("IO error while reading the command,console will be closed");
            }            
            
            String[] inputLine = cmdLine.split(" ");
           
            switch(inputLine[0]){
            	case "upload":
            		handleUp(inputLine);
            		break;
            	case "rm":
            		break;
            	case "download":
            		break;
            	case "cat":
            		break;
            	case "quit":
            		break;
                default:
                    System.out.println(inputLine[0]+"is not a valid command");
            }
        }
	}
	
	//the method to send the message
	public void sendToNN(DFSClientRequest req){
		try {
			oos.writeObject(req);
		} catch (IOException e) {
			System.err.println("DFSClient request sending failed!");
		}
	}
}
