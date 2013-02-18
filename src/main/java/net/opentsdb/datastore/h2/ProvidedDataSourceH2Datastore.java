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

import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * <p>Title: ProvidedDataSourceH2Datastore</p>
 * <p>Description: An extension of {@link H2Datastore} that uses an externally provided {@link DataSource}.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.datastore.h2.ProvidedDataSourceH2Datastore</code></p>
 */

public class ProvidedDataSourceH2Datastore extends Datastore {
	/** The provided datasource */
	protected final DataSource dataSource;
	/** Static class logger */
	public static final Logger logger = LoggerFactory.getLogger(ProvidedDataSourceH2Datastore.class);
	/** Connection that holds the database open */
	private Connection m_holdConnection;  
	
	
	/**
	 * Creates a new ProvidedDataSourceH2Datastore
	 * @param dataSource The provided datasource
	 * @throws DatastoreException thrown on errors setting up the datastore
	 */
	public ProvidedDataSourceH2Datastore(DataSource dataSource) throws DatastoreException {
		this.dataSource = dataSource;
		try {
			m_holdConnection = this.dataSource.getConnection();
			GenOrmDataSource.setDataSource(new DSEnvelope(ds));

			try
			{
				if (createDB)
					createDatabase(ds);
			}
			catch (SQLException e)
			{
				//TODO
				System.out.println("Oh Crap");
				e.printStackTrace();
			}
			catch (IOException e)
			{
				//TODO
				System.out.println("double oh crap");
				e.printStackTrace();
			}
			
		} catch (Exception ex) {
			throw new DatastoreException("Failed to initialize ProvidedDataSourceH2Datastore", ex);
		}
	}
	
	private void createDatabase(DataSource ds) throws IOException, SQLException
	{
		logger.info("Creating DB");
		m_holdConnection = ds.getConnection();
		m_holdConnection.setAutoCommit(false);

		StringBuilder sb = new StringBuilder();
		InputStreamReader reader = new InputStreamReader(getClass().getClassLoader()
				.getResourceAsStream("net/opentsdb/datastore/h2/orm/create.sql"));

		int ch;
		while ((ch = reader.read()) != -1)
			sb.append((char) ch);

		String[] tableCommands = sb.toString().split(";");

		Statement s = m_holdConnection.createStatement();
		for (String command : tableCommands)
			s.execute(command);

		m_holdConnection.commit();
	}

	@Override
	public void close()
	{
		try
		{
			m_holdConnection.close();
		}
		catch (SQLException e)
		{
			logger.error("Failed closing last connection:", e);
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
				if (dataPoint.isInteger())
					dbDataPoint.setLongValue(dataPoint.getLongValue());
				else
					dbDataPoint.setDoubleValue(dataPoint.getDoubleValue());
			}

			GenOrmDataSource.commit();
		}
		finally
		{
			GenOrmDataSource.close();
		}

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
	

}
