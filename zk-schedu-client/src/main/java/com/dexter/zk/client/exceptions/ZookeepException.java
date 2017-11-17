package com.dexter.zk.client.exceptions;

public class ZookeepException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public ZookeepException(){
		super();
	}
	
	public ZookeepException(String msg, Exception e){
		super(msg,e);
	}
	
	public ZookeepException(Exception e){
		super(e);
	}
	
	public ZookeepException(String msg){
		super(msg);
	}
}
