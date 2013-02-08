// OpenTSDB2
// Copyright (C) 2013 Proofpoint, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>

package net.opentsdb.core.telnet;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import net.opentsdb.core.ProtocolService;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringEncoder;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class TelnetServer extends SimpleChannelUpstreamHandler implements ChannelPipelineFactory, ProtocolService, ChannelFactory
{
	private int m_port;
	private Map<String, TelnetCommand> m_commands;
	private UnknownCommand m_unknownCommand;
	private ServerBootstrap bootstrap = null;
	private ChannelFactory delegate = null;
	private ChannelGroup channelGroup = new DefaultChannelGroup();

	@Inject
	public TelnetServer(@Named("opentsdb.telnetserver.port")int port,
			@Named("opentsdb.telnetserver.commands")String commands,
			CommandProvider commandProvider)
	{
		m_commands = new HashMap<String, TelnetCommand>();
		m_port = port;
		String[] splitCommands = commands.split(",");

		for (String cmd : splitCommands)
		{
			TelnetCommand telnetCommand = commandProvider.getCommand(cmd);
			if (telnetCommand != null)
				m_commands.put(cmd, telnetCommand);
		}
	}

	public void run()
	{
		// Configure the server.
		bootstrap = new ServerBootstrap(
				new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));
		
		

		// Configure the pipeline factory.
		bootstrap.setPipelineFactory(this);
		bootstrap.setOption("child.tcpNoDelay", true);
		bootstrap.setOption("child.keepAlive", true);
		bootstrap.setOption("reuseAddress", true);
		delegate = bootstrap.getFactory();
		bootstrap.setFactory(this);
		// Bind and start to accept incoming connections.
		bootstrap.bind(new InetSocketAddress(m_port));
	}

	@Override
	public ChannelPipeline getPipeline() throws Exception
	{
		ChannelPipeline pipeline = Channels.pipeline();

		// Add the text line codec combination first,
		DelimiterBasedFrameDecoder frameDecoder = new DelimiterBasedFrameDecoder(
				1024, Delimiters.lineDelimiter());
		pipeline.addLast("framer", frameDecoder);
		pipeline.addLast("decoder", new WordSplitter());
		pipeline.addLast("encoder", new StringEncoder());

		// and then business logic.
		pipeline.addLast("handler", this);

		return pipeline;
	}
	
	/**
	 * Stops the telnet server and releases its resources
	 */
	public void stop() {
		channelGroup.close();
		bootstrap.releaseExternalResources();
	}

	@Override
	public void messageReceived(final ChannelHandlerContext ctx,
			final MessageEvent msgevent) {
		try {
			final Object message = msgevent.getMessage();
			if (message instanceof String[])
			{
				String[] command = (String[])message;
				TelnetCommand telnetCommand = m_commands.get(command[0]);
				if (telnetCommand == null)
					telnetCommand = m_unknownCommand;

				telnetCommand.execute(msgevent.getChannel(), command);
			} else {
				//TODO
				/*logError(msgevent.getChannel(), "Unexpected message type "
						+ message.getClass() + ": " + message);
				exceptions_caught.incrementAndGet();*/
			}
		} catch (Exception e) {
			Object pretty_message = msgevent.getMessage();
			if (pretty_message instanceof String[]) {
				pretty_message = Arrays.toString((String[]) pretty_message);
			}
			//TODO
			/*logError(msgevent.getChannel(), "Unexpected exception caught"
					+ " while serving " + pretty_message, e);
			exceptions_caught.incrementAndGet();*/
		}
	}

	/**
	 * {@inheritDoc}
	 * @see org.jboss.netty.channel.ChannelFactory#newChannel(org.jboss.netty.channel.ChannelPipeline)
	 */
	@Override
	public Channel newChannel(ChannelPipeline pipeline) {		
		Channel channel = delegate.newChannel(pipeline);
		channelGroup.add(channel);
		return channel;
	}

	/**
	 * {@inheritDoc}
	 * @see org.jboss.netty.channel.ChannelFactory#releaseExternalResources()
	 */
	@Override
	public void releaseExternalResources() {
		delegate.releaseExternalResources();		
	}

	/**
	 * {@inheritDoc}
	 * @see org.jboss.netty.channel.ChannelFactory#shutdown()
	 */
	@Override
	public void shutdown() {
		delegate.shutdown();
	}
}
