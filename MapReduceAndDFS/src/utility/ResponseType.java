package utility;

/**
* This file define the command type used to send from manager to worker and 
* the response type used to send from worker to manager
*/

public enum ResponseType {
    STARTRES,
    KILLTASKRES,/*manager to source worker*/
    CLEANRSP,
    
}