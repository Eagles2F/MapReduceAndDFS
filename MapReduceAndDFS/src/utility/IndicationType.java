package utility;

/**
* This file define the command type used to send from manager to worker and 
* the response type used to send from worker to manager
* @Author Yifan Li
* @Author Jian Wang
*/

public enum IndicationType {
    HEARTBEAT,
    TASKCOMPLETE,
    TASKFAIL,
    GETFILESCOMPLETE //this is send from the dataNode to comfirm the complete of downloading
                    //the mapper output
    
}