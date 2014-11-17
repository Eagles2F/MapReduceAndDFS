package dfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import utility.DFSCommandId;
import utility.DFSMessage;

public class HandleDFSClientReq implements Runnable{

	private Socket soc;
	ObjectInputStream ois;
	ObjectOutputStream oos;
	private NameNode nn;
	public HandleDFSClientReq(Socket soc,NameNode nn){
		this.soc = soc;
		try {
			this.ois = new ObjectInputStream(this.soc.getInputStream());
		} catch (IOException e1) {		
			e1.printStackTrace();
		}
		try {
			this.oos = new ObjectOutputStream(this.soc.getOutputStream());
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.nn = nn;
	}

	@Override
	public void run() {
		
		//read the request
		try {
			DFSClientRequest req = (DFSClientRequest)ois.readObject();
			
			switch(req.getReqType()){
				case InputUpload:
					handleInputUpload(req);
					break;
				default:
					System.out.println("Undefined request");
			
			}
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public void handleInputUpload(DFSClientRequest req){
		//Create a DFSInput File
		DFSInputFile dif = new DFSInputFile(req.getFileName());
		
		dif.setNumOfRecords(req.getFileLineNum());
		
		//Chop the DFSInput File off by the number of DataNodes
		int sumOfAliveDataNodes = 0 ;
		for(int k:nn.getMaster().workerStatusMap.keySet()){
			if(nn.getMaster().workerStatusMap.get(k).isAlive()){
				sumOfAliveDataNodes++;
			}
		}
		if(sumOfAliveDataNodes == 0){
			// deal with this problem
			return;
		}
		
		int numOfChunks =sumOfAliveDataNodes-1;//save one datanode 
		int start_id = 0;
		int sizePerChunk =req.getFileLineNum() / numOfChunks;
		for(int i=0; i<numOfChunks; i++){
			Range r = new Range(start_id,start_id+sizePerChunk);
			DFSFile f = new DFSFile(req.getFileName());
			f.setNodeAddress(nn.getMaster().workerSocMap.get(i).getInetAddress().toString());
			f.setPortNum(11114); ////data node communication port ,should be from the conf file
			f.setNodeLocalFilePath(".."); // should be up to conf
			f.setNodeId(i);
			// set duplication here unfinished
			
			dif.getFileChunks().put(r,f);
			start_id = start_id+sizePerChunk+1;
		}
		if(req.getFileLineNum()%numOfChunks != 0){ // process the tail part
			Range r = new Range(start_id,req.getFileLineNum()-1);
			DFSFile f = new DFSFile(req.getFileName());
			f.setNodeAddress(nn.getMaster().workerSocMap.get(sumOfAliveDataNodes-1).getInetAddress().toString());
			f.setPortNum(11114); ////data node communication port ,should be from the conf file
			f.setNodeLocalFilePath(".."); // should be up to conf
			f.setNodeId(sumOfAliveDataNodes-1);
			// set duplication here unfinished
			
			dif.getFileChunks().put(r,f);

		}
		
		//Store the DFSInput File in the NameNode Directory, assuming all DFSInputFile are stored in the rootDir
		nn.getRootDir().createSubEntry(dif);
		
		//tell the destination node to download the chunks from the target client socket and file path
		for(Range key:dif.getFileChunks().keySet()){
			int nodeId = dif.getFileChunks().get(key).getNodeId();
			DFSMessage msg = new DFSMessage();
			msg.setMessageType(DFSMessage.msgType.COMMAND);
			msg.setCmdId(DFSCommandId.GETFILES);
			msg.setStartIndex();
			msg.setChunkLenth(chunkLenth);
			msg.setTargetNodeAddr(targetNodeAddr);
			msg.setTargetPortNum(targetPortNum);
			msg.setSourceFileName(dif.getName());
		}
		
		
		
	}
}
