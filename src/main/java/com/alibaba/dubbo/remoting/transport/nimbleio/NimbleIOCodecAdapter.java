/*
 * Copyright 1999-2011 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.remoting.transport.nimbleio;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.Codec2;
import com.alibaba.dubbo.remoting.buffer.ChannelBuffer;
import com.alibaba.dubbo.remoting.buffer.ChannelBuffers;
import com.gifisan.nio.common.CloseUtil;
import com.gifisan.nio.component.TCPEndPoint;
import com.gifisan.nio.component.protocol.ProtocolDecoder;
import com.gifisan.nio.component.protocol.ProtocolEncoder;
import com.gifisan.nio.component.protocol.ProtocolFactory;
import com.gifisan.nio.component.protocol.future.IOReadFuture;
import com.gifisan.nio.component.protocol.future.IOWriteFuture;
import com.gifisan.nio.component.protocol.future.ReadFuture;
import com.gifisan.nio.component.protocol.future.TextWriteFuture;

/**
 * MinaCodecAdapter.
 *
 * @author qian.lei
 */
final class NimbleIOCodecAdapter implements ProtocolFactory {

	private final ProtocolEncoder encoder = new InternalEncoder();

	private final ProtocolDecoder decoder = new InternalDecoder();

	private final Codec2 codec;

	private final URL url;

	private final ChannelHandler handler;

	public NimbleIOCodecAdapter(Codec2 codec, URL url, ChannelHandler handler) {
		this.codec = codec;
		this.url = url;
		this.handler = handler;
	}

	public ProtocolEncoder getEncoder() {
		return encoder;
	}

	public ProtocolDecoder getDecoder() {
		return decoder;
	}

	private class InternalEncoder implements ProtocolEncoder {

		public IOWriteFuture encode(TCPEndPoint arg0, ReadFuture arg1)
				throws IOException {
			DubboReadFuture f = (DubboReadFuture) arg1;
			
			ChannelBuffer buffer = ChannelBuffers.dynamicBuffer(1024);
			NimbleioChannel channel = NimbleioChannel.getOrAddChannel(
					arg0.getSession(), codec, url, handler);
			try {
				codec.encode(channel, buffer, f.getMsg());
			} finally {
				NimbleioChannel.removeChannelIfDisconnectd(arg0.getSession());
			}
			
			ByteBuffer buffer2 = buffer.toByteBuffer();
			
			return new TextWriteFuture(arg0, arg1, buffer2);
		}

	}

	private class InternalDecoder implements ProtocolDecoder {

		public IOReadFuture decode(TCPEndPoint arg0) throws IOException {

			ByteBuffer buffer = ByteBuffer.allocate(1024 * 4);

			int length = arg0.read(buffer);

			if (length < 1) {
				if (length == -1) {
					CloseUtil.close(arg0);
				}
				return null;
			}
			return new DubboReadFuture(arg0.getSession(), buffer, codec, url,
					handler);
		}
	}

	public ProtocolDecoder getProtocolDecoder() {
		return decoder;
	}

	public ProtocolEncoder getProtocolEncoder() {
		return encoder;
	}

}