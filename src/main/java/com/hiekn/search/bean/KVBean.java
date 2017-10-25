package com.hiekn.search.bean;

public class KVBean <W,T>{

	private W k;
	private T v;
	private String d;
	
	public W getK() {
		return k;
	}
	public void setK(W k) {
		this.k = k;
	}
	public T getV() {
		return v;
	}
	public void setV(T v) {
		this.v = v;
	}
	public String getD() {
		return d;
	}
	public void setD(String d) {
		this.d = d;
	}
	
}
