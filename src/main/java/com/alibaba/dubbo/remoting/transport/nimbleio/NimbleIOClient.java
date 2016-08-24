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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.transport.AbstractClient;
import com.gifisan.nio.common.CloseUtil;
import com.gifisan.nio.component.DefaultNIOContext;
import com.gifisan.nio.component.NIOContext;
import com.gifisan.nio.component.Session;
import com.gifisan.nio.connector.TCPConnector;
import com.gifisan.nio.extend.configuration.ServerConfiguration;

/**
 * Mina client.
 * 
 * @author qian.lei
 * @author william.liangf
 */
public class NimbleIOClient extends AbstractClient {

	private static Logger					logger		= LoggerFactory.getLogger(NimbleIOClient.class);

	private static Map<String, TCPConnector>	connectors	= new ConcurrentHashMap<String, TCPConnector>();

	private String							connectorKey;

	private TCPConnector					connector;

	private Session						session;												// volatile,
																							// please
	public NimbleIOClient(final URL url, final ChannelHandler handler) throws RemotingException {
		super(url, wrapChannelHandler(url, handler));
	}

	@Override
	protected void doOpen() throws Throwable {
		connectorKey = getUrl().toFullString();
		TCPConnector c = connectors.get(connectorKey);
		if (c != null) {
			connector = c;
		} else {
			NIOContext context = new DefaultNIOContext();
			connector = new TCPConnector();
			connector.setContext(context);

			ServerConfiguration cfg = new ServerConfiguration();

			cfg.setSERVER_HOST(getRemoteAddress().getHostName());
			cfg.setSERVER_TCP_PORT(getRemoteAddress().getPort());

			context.setProtocolFactory(new NimbleIOCodecAdapter(getCodec(), getUrl(), this));

			context.setServerConfiguration(cfg);

			context.setIOEventHandleAdaptor(new NimbleIOHandler(getCodec(), getUrl(), this));

			connectors.put(connectorKey, connector);
		}
	}

	@Override
	protected void doConnect() throws Throwable {

		connector.connect();

		session = connector.getSession();
	}

	@Override
	protected void doDisConnect() throws Throwable {
		try {
			NimbleioChannel.removeChannelIfDisconnectd(session);
		} catch (Throwable t) {
			logger.warn(t.getMessage());
		}
	}

	@Override
	protected void doClose() throws Throwable {
		CloseUtil.close(connector);
	}

	@Override
	protected Channel getChannel() {
		Session s = session;
		if (s == null || !s.isOpened())
			return null;
		return NimbleioChannel.getOrAddChannel(s, getCodec(), getUrl(), this);
	}

}