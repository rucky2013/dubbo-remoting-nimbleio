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

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.Codec2;
import com.alibaba.dubbo.remoting.RemotingException;
import com.gifisan.nio.component.IOEventHandleAdaptor;
import com.gifisan.nio.component.Session;
import com.gifisan.nio.component.protocol.future.ReadFuture;

/**
 * MinaHandler
 * 
 * @author william.liangf
 */
public class NimbleIOHandler extends IOEventHandleAdaptor {

	private final URL			url;

	private final ChannelHandler	handler;

	private final Codec2		codec;

	public NimbleIOHandler(Codec2 codec, URL url, ChannelHandler handler) {
		if (url == null) {
			throw new IllegalArgumentException("url == null");
		}
		if (handler == null) {
			throw new IllegalArgumentException("handler == null");
		}
		if (codec == null) {
			throw new IllegalArgumentException("codec == null");
		}
		this.url = url;
		this.handler = handler;
		this.codec = codec;
	}

	@Override
	public void acceptAlong(Session session, ReadFuture future) throws Exception {
		NimbleioChannel channel = NimbleioChannel.getOrAddChannel(session, codec, url, handler);

		DubboReadFuture f = (DubboReadFuture) future;

		handler.received(channel, f.getMsg());
	}

	public void exceptionCaught(Session session, ReadFuture future, Exception cause, IOEventState state) {
		super.exceptionCaught(session, future, cause, state);
	}

	public void futureSent(Session session, ReadFuture future) {
		NimbleioChannel channel = NimbleioChannel.getOrAddChannel(session, codec, url, handler);

		DubboReadFuture f = (DubboReadFuture) future;

		try {
			handler.sent(channel, f.getWriteBuffer());
		} catch (RemotingException e) {
			e.printStackTrace();
		}
	}

}