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

import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.ObjectName;
import javax.sql.DataSource;

import net.opentsdb.core.DataPointSet;
import net.opentsdb.core.datastore.CachedSearchResult;
import net.opentsdb.core.datastore.Datastore;
import net.opentsdb.core.datastore.DatastoreMetricQuery;
import net.opentsdb.core.datastore.TaggedDataPoints;
import net.opentsdb.core.exception.DatastoreException;
import net.opentsdb.datastore.h2.orm.DSEnvelope;
import net.opentsdb.datastore.h2.orm.DataPoint;
import net.opentsdb.datastore.h2.orm.GenOrmDataSource;
import net.opentsdb.datastore.h2.orm.Metric;
import net.opentsdb.datastore.h2.orm.MetricNamesQuery;
import net.opentsdb.datastore.h2.orm.MetricTag;
import net.opentsdb.datastore.h2.orm.Tag;
import net.opentsdb.datastore.h2.orm.TagNamesQuery;
import net.opentsdb.datastore.h2.orm.TagValuesQuery;
import net.opentsdb.datastore.h2.orm.TagsInQueryData;
import net.opentsdb.datastore.h2.orm.TagsInQueryQuery;

import org.apache.log4j.Logger;


/**
 * <p>Title: ProvidedDataSourceH2Datastore</p>
 * <p>Description: An extension of {@link H2Datastore} that uses an externally provided {@link DataSource}.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.datastore.h2.ProvidedDataSourceH2Datastore</code></p>
 */

public class ProvidedDataSourceH2Datastore extends Datastore implements ProvidedDataSourceH2DatastoreMBean, Runnable {
	/** The provided datasource */
	protected DataSource dataSource = null;
	/** Static class logger */
	public static final Logger logger = Logger.getLogger(ProvidedDataSourceH2Datastore.class);

	/** Datapoint insertion counter */
	protected final AtomicLong dataPointCounter = new AtomicLong(0L);
	
	protected Thread queueWorker = null;
	protected static final AtomicInteger serial = new AtomicInteger();
	
	/**
	 * Creates a new ProvidedDataSourceH2Datastore
	 * @throws DatastoreException thrown on errors setting up the datastore
	 */
	public ProvidedDataSourceH2Datastore() throws DatastoreException {		
		super();		
	}
	
	/**
	 * Registers the management interface.
	 */
	protected void registerMBean() {
		try {
			ObjectName on = new ObjectName(getClass().getPackage().getName() +  ":datastore=" + getClass().getSimpleName());
			if(ManagementFactory.getPlatformMBeanServer().isRegistered(on)) {
				ManagementFactory.getPlatformMBeanServer().unregisterMBean(on);
			}
			ManagementFactory.getPlatformMBeanServer().registerMBean(this, on);
			logger.info("Registered EmbeddedOpenTSDB Management Interface [" + on + "]");
		} catch (Exception ex) {
			logger.warn("Failed to register management interface", ex);
		}
	}
	
	/**
	 * Starts this data store
	 */
	public void start() {
		//registerMBean();
		GenOrmDataSource.setDataSource(new DSEnvelope(dataSource));
		started.set(true);
		queueWorker = new Thread(this, "H2DataPointWriter#" + serial.incrementAndGet());
		queueWorker.setDaemon(true);
		queueWorker.start();
	}
	
	public void run() {
		while(isStarted()) {
			try {
				Collection<DataPointSet> drain = new HashSet<DataPointSet>(100);
				DataPointSet dps = null;
				do {
					dps = dataPointQueue.poll(500, TimeUnit.MILLISECONDS);
					if(dps!=null) drain.add(dps);
				} while(dps!=null && drain.size()<100);
				if(!drain.isEmpty()) {
					dequeued.addAndGet(drain.size());
					putDataPoints(drain);
				}
			} catch (InterruptedException ief) {
				Thread.interrupted();
			} catch (Exception ex) {
				logger.error("Failed to write datapoint set", ex);						
			}
		}
	}
	
	/**
	 * Sets the DataSource to be used by this data store
	 * @param dataSource the DataSource to be used by this data store
	 */
	public void setDataSource(DataSource dataSource) {
		if(dataSource==null) throw new IllegalArgumentException("Passed datasource was null", new Throwable());
		this.dataSource = dataSource;
		Connection conn = null;
		try {
			conn = this.dataSource.getConnection();
			String dbUrl = conn.getMetaData().getURL();
			logger.info("\n\t==================================\n\tEmbedded-OpenTSDB DataSource:" + dbUrl + "\n\t==================================\n");
			System.err.println("\n\t==================================\n\tEmbedded-OpenTSDB DataSource:" + dbUrl + "\n\t==================================\n");
		} catch (Exception ex) {
			throw new RuntimeException("Provided DataSource Failed", ex);
		} finally {
			if(conn!=null) try { conn.close(); } catch (Exception x) { /* No Op */ }
		}		
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.datastore.h2.ProvidedDataSourceH2DatastoreMBean#getDataStoreURL()
	 */
	public String getDataStoreURL() {
		Connection conn = null;
		try {
			conn = this.dataSource.getConnection();
			return conn.getMetaData().getURL();			
		} catch (Exception ex) {
			throw new RuntimeException("Provided DataSource Failed", ex);
		} finally {
			if(conn!=null) try { conn.close(); } catch (Exception x) { /* No Op */ }
		}				
	}
	
	

	public void putDataPoints(DataPointSet dps)
	{
		GenOrmDataSource.attachAndBegin();
		try
		{
			String key = createMetricKey(dps);
			Metric m = Metric.factory.findOrCreate(key);
			m.setName(dps.getName());

			SortedMap<String, String> tags = dps.getTags();
			for (String name : tags.keySet())
			{
				String value = tags.get(name);
				Tag.factory.findOrCreate(name, value);
				MetricTag.factory.findOrCreate(key, name, value);
			}

			for (net.opentsdb.core.DataPoint dataPoint : dps.getDataPoints())
			{
				DataPoint dbDataPoint = DataPoint.factory.createWithGeneratedKey();
				dbDataPoint.setMetricRef(m);
				dbDataPoint.setTimestamp(new Timestamp(dataPoint.getTimestamp()));
				if (dataPoint.isInteger()) {
					dbDataPoint.setLongValue(dataPoint.getLongValue());					
				} else {
					dbDataPoint.setDoubleValue(dataPoint.getDoubleValue());
				}
				dataPointCounter.incrementAndGet();
			}

			GenOrmDataSource.commit();
		}
		finally
		{
			GenOrmDataSource.close();
		}

	}
	
	public void putDataPoints(Collection<DataPointSet> dpColl) {
		if(dpColl==null || dpColl.isEmpty()) return;		
		GenOrmDataSource.attachAndBegin();
		try {
			for(DataPointSet dps: dpColl) {
				String key = createMetricKey(dps);
				Metric m = Metric.factory.findOrCreate(key);
				m.setName(dps.getName());
	
				SortedMap<String, String> tags = dps.getTags();
				for (String name : tags.keySet()) {
					String value = tags.get(name);
					Tag.factory.findOrCreate(name, value);
					MetricTag.factory.findOrCreate(key, name, value);
				}
	
				for (net.opentsdb.core.DataPoint dataPoint : dps.getDataPoints()) {
					DataPoint dbDataPoint = DataPoint.factory.createWithGeneratedKey();
					dbDataPoint.setMetricRef(m);
					dbDataPoint.setTimestamp(new Timestamp(dataPoint.getTimestamp()));
					if (dataPoint.isInteger()) {
						dbDataPoint.setLongValue(dataPoint.getLongValue());					
					} else {
						dbDataPoint.setDoubleValue(dataPoint.getDoubleValue());
					}
					dataPointCounter.incrementAndGet();
				}
			}
			GenOrmDataSource.commit();
		} finally {
			GenOrmDataSource.close();
		}
	}

			
	
	/**
	 * Returns the number of data points inserted.
	 * @return the number of data points inserted.
	 */
	public long getDataPointCount() {
		return dataPointCounter.get();
	}

	@Override
	public Iterable<String> getMetricNames()
	{
		MetricNamesQuery query = new MetricNamesQuery();
		MetricNamesQuery.ResultSet results = query.runQuery();

		List<String> metricNames = new ArrayList<String>();
		while (results.next())
		{
			metricNames.add(results.getRecord().getName());
		}

		results.close();

		return (metricNames);
	}

	@Override
	public Iterable<String> getTagNames()
	{
		TagNamesQuery.ResultSet results = new TagNamesQuery().runQuery();

		List<String> tagNames = new ArrayList<String>();
		while (results.next())
			tagNames.add(results.getRecord().getName());

		results.close();

		return (tagNames);
	}

	@Override
	public Iterable<String> getTagValues()
	{
		TagValuesQuery.ResultSet results = new TagValuesQuery().runQuery();

		List<String> tagValues = new ArrayList<String>();
		while (results.next())
			tagValues.add(results.getRecord().getValue());

		results.close();

		return (tagValues);
	}

	@Override
	protected List<TaggedDataPoints> queryDatabase(DatastoreMetricQuery query, CachedSearchResult cachedSearchResult)
	{
		StringBuilder sb = new StringBuilder();

		//Manually build the where clause for the tags
		//This is subject to sql injection
		for (String tag : query.getTags().keySet())
		{
			sb.append(" and mt.\"tag_name\" = '").append(tag);
			sb.append("' and mt.\"tag_value\" = '").append(query.getTags().get(tag));
			sb.append("'");
		}

		DataPoint.ResultSet results = DataPoint.factory.getForMetric(query.getName(),
				new Timestamp(query.getStartTime()),
				new Timestamp(query.getEndTime()),
				sb.toString());

		TagsInQueryQuery.ResultSet tagsQueryResults = new TagsInQueryQuery(query.getName(),
				new Timestamp(query.getStartTime()),
				new Timestamp(query.getEndTime()),
				sb.toString()).runQuery();

		Map<String, String> tags = new TreeMap<String, String>();
		while (tagsQueryResults.next())
		{
			TagsInQueryData data = tagsQueryResults.getRecord();
			tags.put(data.getTagName(), data.getTagValue());
		}

		H2DataPointGroup dpGroup = new H2DataPointGroup(tags, results);

		return (Collections.singletonList((TaggedDataPoints)dpGroup));

	}

	private String createMetricKey(DataPointSet dps)
	{
		StringBuilder sb = new StringBuilder();
		sb.append(dps.getName()).append(":");

		SortedMap<String, String> tags = dps.getTags();
		for (String name : tags.keySet())
		{
			sb.append(name).append("=");
			sb.append(tags.get(name)).append(":");
		}

		return (sb.toString());
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.core.datastore.Datastore#close()
	 */
	@Override
	public void close() throws InterruptedException, DatastoreException {
		started.set(false);		
		queueWorker.interrupt();
		queueWorker = null;
	}
	

}
