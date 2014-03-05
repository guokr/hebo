package com.guokr.hebo.server;

import com.guokr.hebo.HeboCallback;

public class PipedCallback extends HeboCallback {

	private int order;
	private ServerCallback root;

	public PipedCallback(ServerCallback root, int order) {
		this.order = order;
		this.root = root;
	}

	@Override
	public void response() {
		if (buffer == null) {
			error("Unknown server error!");
		}

		root.buffers[order] = buffer;
		buffer.flip();
		root.response();
	}

}
