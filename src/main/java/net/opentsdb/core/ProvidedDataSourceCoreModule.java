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

import javax.sql.DataSource;

import net.opentsdb.core.datastore.Datastore;
import net.opentsdb.core.telnet.CommandProvider;
import net.opentsdb.core.telnet.GuiceCommandProvider;
import net.opentsdb.core.telnet.PutCommand;
import net.opentsdb.core.telnet.TelnetCommand;
import net.opentsdb.core.telnet.TelnetServer;
import net.opentsdb.datastore.h2.ProvidedDataSourceH2Datastore;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.name.Names;

/**
 * <p>Title: ProvidedDataSourceCoreModule</p>
 * <p>Description: A replacement for {@link CoreModule} for use when an H2 {@link DataSource} is externally provided.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.core.ProvidedDataSourceCoreModule</code></p>
 */

public class ProvidedDataSourceCoreModule extends AbstractModule {
	/** The provided data source */
	protected final DataSource dataSource;
	/** The module configuration properties */
	protected final Properties m_props;
	/**
	 * Creates a new ProvidedDataSourceCoreModule
	 * @param props The configuration properties
	 * @param dataSource The provided data source
	 */
	public ProvidedDataSourceCoreModule(Properties props, DataSource dataSource) {
		this.dataSource = dataSource;
		m_props = props;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.google.inject.AbstractModule#configure()
	 */	
	@Override
	protected void configure() 	{
		bind(TelnetServer.class).in(Singleton.class);
		bind(TelnetCommand.class).annotatedWith(Names.named("put")).to(PutCommand.class);
		bind(CommandProvider.class).to(GuiceCommandProvider.class);
//		ProvidedDataSourceH2Datastore datastore = null;
//		try {
//			datastore = new ProvidedDataSourceH2Datastore(this.dataSource);
//		} catch (Exception ex) {
//			throw new RuntimeException("Failed to create ProvidedDataSourceH2Datastore", ex);
//		}
		Names.bindProperties(binder(), m_props);
	}
	
	
		

}
