/**
 * Helios, OpenSource Monitoring
 * Brought to you by the Helios Development Group
 *
 * Copyright 2007, Helios Development Group and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org. 
 *
 */
package net.opentsdb.core;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import net.opentsdb.core.http.WebServletModule;
import net.opentsdb.core.telnet.TelnetServer;

import org.eclipse.jetty.server.Server;

import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * <p>Title: MainBean</p>
 * <p>Description: Beanified launcher to boot up opentsdb within another JVM</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.core.MainBean</code></p>
 */

public class MainBean {
	/** The properties to initialize with */
	protected Properties configProperties = new Properties();
	/** Flag to indicate if the opentsdb2 engine started up */
	protected final AtomicBoolean started = new AtomicBoolean(false);
	/** The guice injector */
	protected Injector injector = null;
	
	protected TelnetServer ts = null;
	/**
	 * Starts the opentsdb2 engine
	 * @throws Exception thrown on any exception
	 */
	public void start() throws Exception {
		try {
			injector = createGuiceInjector();
			startTelnetListener();
			startWebServer();
			started.set(true);
		} catch (Exception ex) {
			started.set(false);			
			ex.printStackTrace(System.err);
			throw ex;
		}
	}
	
	/**
	 * Stops the opentsdb2 engine
	 */
	public void stop() {
		
	}
	
	/**
	 * Indicates if the opentsdb2 engine is started
	 * @return true if the opentsdb2 engine is started, false otherwise 
	 */
	public boolean isStarted() {
		return started.get();
	}
	
	/**
	 * Sets the properties for the opentsdb2 launcher
	 * @param props the properties for the opentsdb2 launcher
	 */
	public void setProperties(Properties props) {
		if(props!=null) {
			configProperties.putAll(props);
		}
	}
	
	/**
	 * Creates the guice injector using the configured properties
	 * @return the guice injector
	 */
	public Injector createGuiceInjector()  {
		Injector injector = Guice.createInjector(new CoreModule(configProperties), new WebServletModule());
		return (injector);
	}
	
	/**
	 * Starts the opentsdb telnet listener
	 */
	public void startTelnetListener() {
		ts = injector.getInstance(TelnetServer.class);
		ts.run();
		//System.out.println("Done run");
	}

	/**
	 * Starts the opentsdb2 web server
	 * @throws Exception thrown on any exception
	 */
	public void startWebServer() throws Exception 	{
		Server server = injector.getInstance(Server.class);
		//System.out.println("Starting web server");
		server.start();		
	}
	
	
}
