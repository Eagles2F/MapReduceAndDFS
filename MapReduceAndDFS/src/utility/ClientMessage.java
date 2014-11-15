package utility;

import java.io.Serializable;

// This is the message from master to client
public class ClientMessage implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -2101687996358583125L;
	private int success = 0;
	public ClientMessage(int s){
		this.setSuccess(s);
	}
	public int getSuccess() {
		return success;
	}
	public void setSuccess(int success) {
		this.success = success;
	}
}
