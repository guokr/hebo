package com.guokr.hebo.server;

import java.nio.ByteBuffer;

import com.guokr.hebo.HeboCallback;

public class ServerCallback extends HeboCallback {

	public int pipesize;

	public ByteBuffer[] buffers;
	public RespCallback response;

	public ServerCallback(int size, RespCallback cb) {
		this.response = cb;
		this.pipesize = size;
		this.buffers = new ByteBuffer[size];
	}

	public void response() {
		if (this.pipesize == 1) {
			this.response.run(this.buffers);
		} else {
			this.pipesize--;
		}
	}

}
