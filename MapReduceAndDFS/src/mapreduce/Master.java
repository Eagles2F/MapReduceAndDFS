package mapreduce;

import java.util.concurrent.ConcurrentHashMap;
import java.net.*;
/*
 *Master manages the whole MapReduce facility including client job running, spliting , scheduling, worker node managing,
 *job monitoring and etc.  
 *
 *@Author: Yifan Li
 *@Author: Jian Wang
 *
 *@Date: 11/9/2014
 *@Version:0.00,developing version

 */

class Master{
	//properties
		
		//The HashMap for worker's socket
		public ConcurrentHashMap<Integer,Socket> workerSocMap;
		
		//The HashMap for each worker's manager server 
		public ConcurrentHashMap<Integer,WorkerManagerServer> workerMangerServerMap;
		
		//The HashMap for worker status:-1 represents "dead", 1 represents "alive so far".
		public ConcurrentHashMap<Integer,Integer> workerStatusMap;
		
		private HireWorkerServer hireWorkerServer;
		
		
	//methods
	
	//main process
	public static void main(String[] args){
		//initialization
		
		//start the MapReduceServer thread
		
		//start the HireWorkerServer thread
		
		//start the management shell
	}
}