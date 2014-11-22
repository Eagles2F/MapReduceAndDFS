package dfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import utility.DFSCommandId;
import utility.DFSMessage;
import utility.DFSMessage.DownloadType;
import utility.DFSMessage.nodeType;

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
		
		nn.JobStatusMap.put(req.getJobName(), new DFSJobStatus(dif));
		
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
		
		int start_id = 0;
		int sizePerChunk =req.getFileLineNum() / sumOfAliveDataNodes;
		if(req.getFileLineNum()%sumOfAliveDataNodes == 0){
			for(int i=0; i<sumOfAliveDataNodes; i++){
				Range r = new Range(start_id,start_id+sizePerChunk+1);
				DFSFile f = new DFSFile(req.getFileName());
				f.setNodeAddress(nn.getMaster().workerSocMap.get(i).getInetAddress().getHostAddress());
				f.setNodeLocalFilePath("../DFS/InputChunk"); // should be up to conf
				f.setNodeId(i);
			
				if(sumOfAliveDataNodes != 1){ //set replication file chunk when DataNode number is more than 1 
					f.setDupLocalFilePath("../DFS/InputChunk");
					if(i != sumOfAliveDataNodes-1){
						f.setDupId(i+1);
						f.setDupNodeAddress(nn.dataNodeSocMap.get(i+1).getInetAddress().getHostAddress());
					}else{
						f.setDupId(0);
						f.setDupNodeAddress(nn.dataNodeSocMap.get(0).getInetAddress().getHostAddress());
					}
				}
				
				dif.getFileChunks().put(r,f);
				start_id = start_id+sizePerChunk+1;
			}
		}else{
			for(int i=0; i<sumOfAliveDataNodes-1; i++){
				Range r = new Range(start_id,start_id+sizePerChunk+1);
				DFSFile f = new DFSFile(req.getFileName());
				f.setNodeAddress(nn.getMaster().workerSocMap.get(i).getInetAddress().getHostAddress());
				f.setNodeLocalFilePath("../DFS/InputChunk"); // should be up to conf
				f.setNodeId(i);

				if(sumOfAliveDataNodes != 1){ //set replication file chunk when DataNode number is more than 1 
					f.setDupLocalFilePath("../DFS/InputChunk");
					f.setDupId(i+1);
					f.setDupNodeAddress(nn.dataNodeSocMap.get(i+1).getInetAddress().getHostAddress());
				}
				
				dif.getFileChunks().put(r,f);
				start_id = start_id+sizePerChunk+1;
			}
			Range r = new Range(start_id-1,req.getFileLineNum());
			DFSFile f = new DFSFile(req.getFileName());
			f.setNodeAddress(nn.getMaster().workerSocMap.get(sumOfAliveDataNodes-1).getInetAddress().toString());
			f.setNodeLocalFilePath("../DFS/InputChunk"); // should be up to conf
			f.setNodeId(sumOfAliveDataNodes-1);
			// set duplication here
			if(sumOfAliveDataNodes != 1){ //set replication file chunk when DataNode number is more than 1
				f.setDupLocalFilePath("../DFS/InputChunk");
				f.setDupId(0);
				f.setDupNodeAddress(nn.dataNodeSocMap.get(0).getInetAddress().getHostAddress());
			}
			
			dif.getFileChunks().put(r,f);
		}
		
		//Store the DFSInput File in the NameNode Directory, assuming all DFSInputFile are stored in the rootDir
		nn.getRootDir().createSubEntry(dif);
		
		//tell the destination node to download the chunks from the target client socket and file path
		for(Range key:dif.getFileChunks().keySet()){
			//tell the node to download the file chunk
			DFSFile f = dif.getFileChunks().get(key);
			
			System.out.println("File Chunk "+f.getNodeId()+" Chunk Size:"+(key.endId-key.startId));
			
			DFSMessage msg = new DFSMessage();
			msg.setMessageType(DFSMessage.msgType.COMMAND);
			msg.setCmdId(DFSCommandId.GETFILES);
			msg.setStartIndex(key.startId);
			msg.setChunkLenth(key.endId-key.startId);
			String[] ipAddr = {soc.getInetAddress().getHostAddress()};
			int[] prot = {req.getDownloadServerPort()};
			msg.setTargetCount(1);
			msg.setTargetNodeAddr(ipAddr);
			msg.setTargetPortNum(prot);  // set by the system configuration
			msg.setLocalFileName(f.getName());
			msg.setLocalPath(f.getNodeLocalFilePath());
			msg.setTargetPath(req.getInputFilePath());
			msg.setTargetFileName(req.getFileName());
			msg.setDownloadType(DownloadType.TXT);
			msg.setMessageSource(nodeType.NAMENODE);
			msg.setJobName(req.getJobName());
			try {
				nn.JobStatusMap.get(req.getJobName()).getUploadStatusMap().put(f.getNodeId(), false);
				System.out.println(f.getNodeId());
				nn.dataNodeManagerMap.get(f.getNodeId()).sendToDataNode(msg);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			//tell the node to download the replication of the chunk
			DFSMessage msg1 = new DFSMessage();
			msg1.setMessageType(DFSMessage.msgType.COMMAND);
			msg1.setCmdId(DFSCommandId.GETFILES);
			msg1.setStartIndex(key.startId);
			msg1.setChunkLenth(key.endId-key.startId);
			String[] ipAddr1 = {soc.getInetAddress().getHostAddress()};
			int[] prot1 = {req.getDownloadServerPort()};
			msg1.setTargetCount(1);
			msg1.setTargetNodeAddr(ipAddr1);
			msg1.setTargetPortNum(prot1);  // set by the system configuration
			msg1.setLocalFileName(f.getDuplicationName());
			msg1.setLocalPath(f.getDuplicationName());
			msg1.setTargetPath(req.getInputFilePath());
			msg1.setTargetFileName(req.getFileName());
			msg1.setDownloadType(DownloadType.TXT);
			msg1.setMessageSource(nodeType.NAMENODE);
			msg1.setJobName(req.getJobName());
			try {
				nn.dataNodeManagerMap.get(f.getDupId()).sendToDataNode(msg1);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
}
