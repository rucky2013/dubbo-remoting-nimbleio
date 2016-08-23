package com.alibaba.dubbo.remoting.transport.nimbleio;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.Codec2;
import com.alibaba.dubbo.remoting.buffer.ChannelBuffer;
import com.alibaba.dubbo.remoting.buffer.ChannelBuffers;
import com.gifisan.nio.component.Session;
import com.gifisan.nio.component.protocol.future.AbstractIOReadFuture;

public class DubboReadFuture extends AbstractIOReadFuture {

	private Object msg;

	private ByteBuffer buffer;

	private final Codec2 codec;

	private final URL url;

	private final ChannelHandler handler;

	public DubboReadFuture(Session session, ByteBuffer cache, Codec2 codec,
			URL url, ChannelHandler handler) {
		super(session);
		this.buffer = cache;
		this.url = url;
		this.codec = codec;
		this.handler = handler;
	}
	
	

	public DubboReadFuture(Session session, Object msg, Codec2 codec, URL url,
			ChannelHandler handler) {
		super(session);
		this.msg = msg;
		this.codec = codec;
		this.url = url;
		this.handler = handler;
	}



	public boolean read() throws IOException {
		
		endPoint.read(buffer);
		
		buffer.flip();
		
		ChannelBuffer frame = ChannelBuffers.wrappedBuffer(buffer);

		Channel channel = NimbleioChannel.getOrAddChannel(session,codec, url, handler);
		Object msg;
		int savedReadIndex;

		try {
			do {
				savedReadIndex = frame.readerIndex();
				msg = codec.decode(channel, frame);
				if (msg == Codec2.DecodeResult.NEED_MORE_INPUT) {
					frame.readerIndex(savedReadIndex);
					break;
				} else {
					if (savedReadIndex == frame.readerIndex()) {
						throw new IOException("Decode without read data.");
					}
					if (msg != null) {
						this.msg = msg;

						return true;
					}
				}
			} while (frame.readable());
		} finally {
			if (frame.readable()) {
				frame.discardReadBytes();
			} else {
			}
			NimbleioChannel.removeChannelIfDisconnectd(session);
		}

		return false;
	}

	public String getServiceName() {
		// TODO Auto-generated method stub
		return null;
	}

	public Object getMsg() {
		return msg;
	}
	
}
