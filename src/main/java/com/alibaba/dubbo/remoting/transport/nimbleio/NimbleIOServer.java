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

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.ExecutorUtil;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.transport.AbstractServer;
import com.alibaba.dubbo.remoting.transport.dispatcher.ChannelHandlers;
import com.gifisan.nio.acceptor.TCPAcceptor;
import com.gifisan.nio.component.DefaultNIOContext;
import com.gifisan.nio.component.NIOContext;
import com.gifisan.nio.extend.configuration.ServerConfiguration;

/**
 * MinaServer
 * 
 * @author qian.lei
 * @author william.liangf
 * @author ding.lid
 */
public class NimbleIOServer extends AbstractServer {
    
    private static final Logger logger = LoggerFactory.getLogger(NimbleIOServer.class);

    private TCPAcceptor acceptor;

    public NimbleIOServer(URL url, ChannelHandler handler) throws RemotingException{
        super(url, ChannelHandlers.wrap(handler, ExecutorUtil.setThreadName(url, SERVER_THREAD_POOL_NAME)));
    }

    @Override
    protected void doOpen() throws Throwable {
        // set thread pool.
    	NIOContext context = new DefaultNIOContext();
        acceptor = new TCPAcceptor();
        acceptor.setContext(context);

		ServerConfiguration cfg = new ServerConfiguration();
		
		cfg.setSERVER_TCP_PORT(getBindAddress().getPort());

		context.setServerConfiguration(cfg);
		
		context.setProtocolFactory(new NimbleIOCodecAdapter(getCodec(),
				getUrl(), this));
		
		context.setIOEventHandleAdaptor(new NimbleIOHandler(getCodec(),getUrl(), this));
		
        acceptor.bind();
    }

    @Override
    protected void doClose() throws Throwable {
        try {
            if (acceptor != null) {
                acceptor.unbind();
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
    }

    public Collection<Channel> getChannels() {
//        Set<Session> sessions = acceptor.getManagedSessions(getBindAddress());
        Collection<Channel> channels = new HashSet<Channel>();
//        for (Session session : sessions) {
//            if (session.isConnected()) {
//                channels.add(MinaChannel.getOrAddChannel(session, getUrl(), this));
//            }
//        }
        logger.error(new Exception());
        return channels;
    }

    public Channel getChannel(InetSocketAddress remoteAddress) {
//        Set<Session> sessions = acceptor.getManagedSessions(getBindAddress());
//        for (Session session : sessions) {
//            if (session.getRemoteAddress().equals(remoteAddress)) {
//                return MinaChannel.getOrAddChannel(session, getUrl(), this);
//            }
//        }
    	logger.error(new Exception());
        return null;
    }

    public boolean isBound() {
        return true;
    }

}