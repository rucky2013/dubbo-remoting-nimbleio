package com.alibaba.dubbo.remoting.transport.nimbleio;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.Codec2;
import com.alibaba.dubbo.remoting.buffer.ChannelBuffer;
import com.alibaba.dubbo.remoting.buffer.ChannelBuffers;
import com.gifisan.nio.common.MathUtil;
import com.gifisan.nio.component.Session;
import com.gifisan.nio.component.protocol.AbstractIOReadFuture;

public class DubboReadFuture extends AbstractIOReadFuture {

	private Object				msg;

	private ByteBuffer			buffer;

	private boolean			headerComplete;

	private boolean			bodyComplete;

	private final Codec2		codec;

	private final URL			url;

	private final ChannelHandler	handler;

	public DubboReadFuture(Session session, ByteBuffer header, Codec2 codec, URL url, ChannelHandler handler)
			throws IOException {
		super(session);
		this.buffer = header;
		this.url = url;
		this.codec = codec;
		this.handler = handler;

		if (!header.hasRemaining()) {
			doHeaderComplete(header);
		}
	}

	public DubboReadFuture(Session session, Object msg, Codec2 codec, URL url, ChannelHandler handler) {
		super(session);
		this.msg = msg;
		this.codec = codec;
		this.url = url;
		this.handler = handler;
	}

	private void doHeaderComplete(ByteBuffer header) throws IOException {

		int bodyLength = MathUtil.byte2Int(header.array(), 12);

		if (bodyLength > 1024 * 1024) {
			throw new IOException("max length 1M,length " + bodyLength);
		}

		buffer = ByteBuffer.allocate(bodyLength + 16);

		buffer.put(header.array());

		headerComplete = true;
	}

	private void doBodyComplete(ByteBuffer buffer) throws IOException {

		buffer.flip();

		ChannelBuffer frame = ChannelBuffers.wrappedBuffer(buffer);

		Channel channel = NimbleioChannel.getOrAddChannel(session, codec, url, handler);

		Object msg = codec.decode(channel, frame);

		if (msg == Codec2.DecodeResult.NEED_MORE_INPUT) {
			throw new IOException("system exception");
		} else {
			if (msg == null) {
				throw new IOException("system exception");
			}
			this.msg = msg;
		}

		bodyComplete = true;
	}

	public boolean read() throws IOException {

		if (!headerComplete) {

			endPoint.read(buffer);

			if (!buffer.hasRemaining()) {
				doBodyComplete(buffer);
			}
		}

		ByteBuffer buffer = this.buffer;

		if (!bodyComplete) {

			endPoint.read(buffer);

			if (!buffer.hasRemaining()) {

				doBodyComplete(buffer);
			}
		}

		return true;
	}

	public String getServiceName() {
		return null;
	}

	public Object getMsg() {
		return msg;
	}

}
