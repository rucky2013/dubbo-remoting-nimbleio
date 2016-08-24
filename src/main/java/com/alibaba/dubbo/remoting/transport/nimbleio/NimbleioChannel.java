package com.alibaba.dubbo.remoting.transport.nimbleio;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.Codec2;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.transport.AbstractChannel;
import com.gifisan.nio.component.Session;

public class NimbleioChannel extends AbstractChannel {

	private static final String	CHANNEL_KEY	= NimbleioChannel.class.getName() + ".CHANNEL";

	private Codec2				codec;

	public NimbleioChannel(Session session, Codec2 codec, URL url, ChannelHandler handler) {
		super(url, handler);
		this.session = session;
		this.codec = codec;
	}

	static NimbleioChannel getOrAddChannel(Session session, Codec2 codec, URL url, ChannelHandler handler) {
		if (session == null) {
			return null;
		}
		NimbleioChannel ret = (NimbleioChannel) session.getAttribute(CHANNEL_KEY);
		if (ret == null) {
			synchronized (session) {

				ret = (NimbleioChannel) session.getAttribute(CHANNEL_KEY);

				if (ret != null) {
					return ret;
				}

				ret = new NimbleioChannel(session, codec, url, handler);

				if (session.isOpened()) {

					session.setAttribute(CHANNEL_KEY, ret);
				}
			}
		}
		return ret;
	}

	public void send(Object message, boolean sent) throws RemotingException {
		super.send(message, sent);

		if (message == null) {
			throw new IllegalArgumentException("empty message");
		}

		DubboReadFuture future = new DubboReadFuture(session, message, codec, getUrl(), getChannelHandler());

		try {
			session.flush(future);
		} catch (IOException e) {
			throw new RemotingException(this, e);
		}
	}

	private Session	session;

	public InetSocketAddress getRemoteAddress() {
		return session.getRemoteSocketAddress();
	}

	public boolean isConnected() {
		return session.isOpened();
	}

	public boolean hasAttribute(String key) {
		return session.getAttribute(key) != null;
	}

	public Object getAttribute(String key) {
		return session.getAttribute(key);
	}

	public void setAttribute(String key, Object value) {
		session.setAttribute(key, value);
	}

	public void removeAttribute(String key) {
		session.removeAttribute(key);
	}

	public InetSocketAddress getLocalAddress() {
		return session.getLocalSocketAddress();
	}

	static void removeChannelIfDisconnectd(Session session) {
		if (session != null && !session.isOpened()) {
			session.removeAttribute(CHANNEL_KEY);
		}
	}

}
