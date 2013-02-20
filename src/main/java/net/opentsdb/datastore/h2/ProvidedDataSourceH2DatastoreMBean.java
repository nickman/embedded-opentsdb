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
package net.opentsdb.datastore.h2;

/**
 * <p>Title: ProvidedDataSourceH2DatastoreMBean</p>
 * <p>Description: JMX MBean interface for {@link ProvidedDataSourceH2Datastore}</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.datastore.h2.ProvidedDataSourceH2DatastoreMBean</code></p>
 */

public interface ProvidedDataSourceH2DatastoreMBean {
	/**
	 * Returns the number of data points inserted.
	 * @return the number of data points inserted.
	 */
	public long getDataPointCount();
	
	/**
	 * Returns the data-store's database URL
	 * @return the data-store's database URL
	 */
	public String getDataStoreURL();
	
	/**
	 * Indicates if this data store is started
	 * @return true if this data store is started, false otherwise
	 */
	public boolean isStarted();
	
	/**
	 * Returns the number of enqueued datapoints
	 * @return the number of enqueued datapoints
	 */
	public long getEnqueuedCount();
	
	/**
	 * Returns the number of dequeued datapoints
	 * @return the number of dequeued datapoints
	 */
	public long getDequeuedCount();
	
	/**
	 * Returns the number of dropped datapoints
	 * @return the number of dropped datapoints
	 */
	public long getDroppedCount();
	
	/**
	 * Returns the current queue size
	 * @return the current queue size
	 */
	public int getQueueSize();
	
	
}
