package com.guokr.hebo.server;

import com.guokr.hebo.HeboCallback;

public class ServerCallback extends HeboCallback {

	public RespCallback response;
	public int pipesize;

	public ServerCallback(int size, RespCallback cb) {
		this.response = cb;
		this.pipesize = size;
	}

	public void response() {
		if (this.buffer == null) {
			this.error("Unknown server error!");
		}

		if (this.pipesize == 1) {
			this.buffer.flip();
			this.response.run(this.buffer);
		} else {
			this.pipesize--;
		}
	}

}
