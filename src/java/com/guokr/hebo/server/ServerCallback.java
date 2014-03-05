package com.guokr.hebo.server;

import java.nio.ByteBuffer;

import com.guokr.hebo.HeboCallback;

public class ServerCallback extends HeboCallback {

	public int cur;
	public int pipesize;
	public ByteBuffer[] buffers;
	public RespCallback response;

	public ServerCallback(int size, RespCallback cb) {
		this.response = cb;
		this.cur = 0;
		this.pipesize = size;
		this.buffers = new ByteBuffer[size];
	}

	public void response() {
		if (this.buffer == null) {
			this.error("Unknown server error!");
		}
		buffers[cur++] = this.buffer;

		if (this.pipesize == 1) {
			this.buffer.flip();
			this.response.run(this.buffers);
		} else {
			this.pipesize--;
		}
	}

}
