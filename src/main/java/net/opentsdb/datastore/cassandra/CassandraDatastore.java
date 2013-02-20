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
package net.opentsdb.datastore.cassandra;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPointSet;
import net.opentsdb.core.datastore.*;
import net.opentsdb.core.exception.DatastoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CassandraDatastore extends Datastore
{
	public static final Logger logger = LoggerFactory.getLogger(CassandraDatastore.class);

	public static final int ROW_KEY_CACHE_SIZE = 1024;
	public static final int STRING_CACHE_SIZE = 1024;

	public static final DataPointsRowKeySerializer DATA_POINTS_ROW_KEY_SERIALIZER = new DataPointsRowKeySerializer();
	public static final LongOrDoubleSerializer LONG_OR_DOUBLE_SERIALIZER = new LongOrDoubleSerializer();

	public static final String HOST_NAME_PROPERTY = "opentsdb.datastore.cassandra.host_name";
	public static final String PORT_PROPERTY = "opentsdb.datastore.cassandra.port";
	public static final String REPLICATION_FACTOR_PROPERTY = "opentsdb.datastore.cassandra.replication_factor";
	public static final String ROW_WIDTH_PROPERTY = "opentsdb.datastore.cassandra.row_width";
//	public static final String WRITE_DELAY_PROPERTY = "opentsdb.datastore.cassandra.write_delay";
	public static final String ROW_READ_SIZE_PROPERTY = "opentsdb.datastore.cassandra.row_read_size";

	public static final String KEYSPACE = "opentsdb";
	public static final String CF_DATA_POINTS = "data_points";
	public static final String CF_ROW_KEY_INDEX = "row_key_index";
	public static final String CF_STRING_INDEX = "string_index";

	public static final String ROW_KEY_METRIC_NAMES = "metric_names";
	public static final String ROW_KEY_TAG_NAMES = "tag_names";
	public static final String ROW_KEY_TAG_VALUES = "tag_values";

	private Cluster m_cluster;
	private Keyspace m_keyspace;
	private long m_rowWidth;
	private int m_rowReadSize;
	private WriteBuffer<DataPointsRowKey, Long, LongOrDouble> m_dataPointWriteBuffer;
	private WriteBuffer<String, DataPointsRowKey, String> m_rowKeyWriteBuffer;
	private WriteBuffer<String, String, String> m_stringIndexWriteBuffer;

	private DataCache<DataPointsRowKey> m_rowKeyCache = new DataCache<DataPointsRowKey>(ROW_KEY_CACHE_SIZE);
	private DataCache<String> m_metricNameCache = new DataCache<String>(STRING_CACHE_SIZE);
	private DataCache<String> m_tagNameCache = new DataCache<String>(STRING_CACHE_SIZE);
	private DataCache<String> m_tagValueCache = new DataCache<String>(STRING_CACHE_SIZE);


	@Inject
	public CassandraDatastore(@Named(HOST_NAME_PROPERTY)String cassandraHost,
			@Named(PORT_PROPERTY)String cassandraPort,
			@Named(REPLICATION_FACTOR_PROPERTY)int replicationFactor,
			@Named(ROW_WIDTH_PROPERTY)long rowWidth,
			@Named(ROW_READ_SIZE_PROPERTY)int rowReadSize) throws DatastoreException
	{
		super();
		m_rowWidth = rowWidth;
		m_rowReadSize = rowReadSize;

		m_cluster = HFactory.getOrCreateCluster("tsdb-cluster",
				cassandraHost+":"+cassandraPort);

		KeyspaceDefinition keyspaceDef = m_cluster.describeKeyspace(KEYSPACE);

		if (keyspaceDef == null)
			createSchema(replicationFactor);

		m_keyspace = HFactory.createKeyspace(KEYSPACE, m_cluster);

		m_dataPointWriteBuffer = new WriteBuffer<DataPointsRowKey, Long, LongOrDouble>(
				m_keyspace, CF_DATA_POINTS, 1000,
				DATA_POINTS_ROW_KEY_SERIALIZER,
				LongSerializer.get(),
				LONG_OR_DOUBLE_SERIALIZER,
				new WriteBufferStats()
				{
					@Override
					public void saveWriteSize(int pendingWrites)
					{
						DataPointSet dps = new DataPointSet("opentsdb.datastore.write_size");
						dps.addTag("host", "server");
						dps.addTag("buffer", CF_DATA_POINTS);
						dps.addDataPoint(new DataPoint(System.currentTimeMillis(), pendingWrites));
						putDataPoints(dps);
					}
				});

		m_rowKeyWriteBuffer = new WriteBuffer<String, DataPointsRowKey, String>(
				m_keyspace, CF_ROW_KEY_INDEX, 1000,
				StringSerializer.get(),
				DATA_POINTS_ROW_KEY_SERIALIZER,
				StringSerializer.get(),
				new WriteBufferStats()
				{
					@Override
					public void saveWriteSize(int pendingWrites)
					{
						DataPointSet dps = new DataPointSet("opentsdb.datastore.write_size");
						dps.addTag("host", "server");
						dps.addTag("buffer", CF_ROW_KEY_INDEX);
						dps.addDataPoint(new DataPoint(System.currentTimeMillis(), pendingWrites));
						putDataPoints(dps);
					}
				});

		m_stringIndexWriteBuffer = new WriteBuffer<String, String, String>(
				m_keyspace, CF_STRING_INDEX, 1000,
				StringSerializer.get(),
				StringSerializer.get(),
				StringSerializer.get(),
				new WriteBufferStats()
				{
					@Override
					public void saveWriteSize(int pendingWrites)
					{
						DataPointSet dps = new DataPointSet("opentsdb.datastore.write_size");
						dps.addTag("host", "server");
						dps.addTag("buffer", CF_STRING_INDEX);
						dps.addDataPoint(new DataPoint(System.currentTimeMillis(), pendingWrites));
						putDataPoints(dps);
					}
				});
		started.set(true);
	}

	private void createSchema(int replicationFactor)
	{
		List<ColumnFamilyDefinition> cfDef = new ArrayList<ColumnFamilyDefinition>();

		cfDef.add(HFactory.createColumnFamilyDefinition(
				KEYSPACE, CF_DATA_POINTS, ComparatorType.BYTESTYPE));

		cfDef.add(HFactory.createColumnFamilyDefinition(
				KEYSPACE, CF_ROW_KEY_INDEX, ComparatorType.BYTESTYPE));

		cfDef.add(HFactory.createColumnFamilyDefinition(
				KEYSPACE, CF_STRING_INDEX, ComparatorType.UTF8TYPE));

		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(
				KEYSPACE, ThriftKsDef.DEF_STRATEGY_CLASS,
				replicationFactor, cfDef);

		m_cluster.addKeyspace(newKeyspace, true);
	}


	@Override
	public void close() throws InterruptedException, DatastoreException
	{
		super.close();
		try {
			m_dataPointWriteBuffer.close();
			m_rowKeyWriteBuffer.close();
		} catch (Exception ex) {/*No Op*/}
		
	}

	@Override
	public void putDataPoints(DataPointSet dps)
	{
		try
		{
			long rowTime;
			long nextRow = 0L;
			DataPointsRowKey rowKey = null;

			for (DataPoint dp : dps.getDataPoints())
			{
				if (dp.getTimestamp() > nextRow)
				{
					//System.out.println("Creating new row key");
					rowTime = calculateRowTime(dp.getTimestamp());
					nextRow = rowTime + m_rowWidth;

					rowKey = new DataPointsRowKey(dps.getName(), rowTime, dps.getTags());

					long now = System.currentTimeMillis();
					//Write out the row key if it is not cached
					if (!m_rowKeyCache.isCached(rowKey))
						m_rowKeyWriteBuffer.addData(dps.getName(), rowKey, "", now);

					//Write metric name if not in cache
					if (!m_metricNameCache.isCached(dps.getName()))
					{
						m_stringIndexWriteBuffer.addData(ROW_KEY_METRIC_NAMES,
								dps.getName(), "", now);
					}

					//Check tag names and values to write them out
					Map<String, String> tags = dps.getTags();
					for (String tagName : tags.keySet())
					{
						if (!m_tagNameCache.isCached(tagName))
						{
							m_stringIndexWriteBuffer.addData(ROW_KEY_TAG_NAMES,
									tagName, "", now);
						}

						String value = tags.get(tagName);
						if (!m_tagValueCache.isCached(value))
						{
							m_stringIndexWriteBuffer.addData(ROW_KEY_TAG_VALUES,
									value, "", now);
						}
					}
				}

				//System.out.println("Adding data point value");
				//System.out.println("Inserting "+dp);
				if (dp.isInteger())
				{
					m_dataPointWriteBuffer.addData(rowKey, dp.getTimestamp(),
							new LongOrDouble(dp.getLongValue()), dp.getTimestamp());
				}
				else
				{
					m_dataPointWriteBuffer.addData(rowKey, dp.getTimestamp(),
							new LongOrDouble(dp.getDoubleValue()), dp.getTimestamp());
				}
				dataPointCounter.incrementAndGet();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	@Override
	public Iterable<String> getMetricNames()
	{
		SliceQuery<String, String, String> sliceQuery =
				HFactory.createSliceQuery(m_keyspace, StringSerializer.get(), StringSerializer.get(),
				StringSerializer.get());

		sliceQuery.setColumnFamily(CF_STRING_INDEX);
		sliceQuery.setKey(ROW_KEY_METRIC_NAMES);

		ColumnSliceIterator<String, String, String> columnIterator =
				new ColumnSliceIterator<String, String, String>(sliceQuery, "", (String)null, false, m_rowReadSize);

		List<String> ret = new ArrayList<String>();

		while (columnIterator.hasNext())
			ret.add(columnIterator.next().getName());

		return (ret);
	}

	@Override
	public Iterable<String> getTagNames()
	{
		SliceQuery<String, String, String> sliceQuery =
				HFactory.createSliceQuery(m_keyspace, StringSerializer.get(), StringSerializer.get(),
						StringSerializer.get());

		sliceQuery.setColumnFamily(CF_STRING_INDEX);
		sliceQuery.setKey(ROW_KEY_TAG_NAMES);

		ColumnSliceIterator<String, String, String> columnIterator =
				new ColumnSliceIterator<String, String, String>(sliceQuery, "", (String)null, false, m_rowReadSize);

		List<String> ret = new ArrayList<String>();

		while (columnIterator.hasNext())
			ret.add(columnIterator.next().getName());

		return (ret);
	}

	@Override
	public Iterable<String> getTagValues()
	{
		SliceQuery<String, String, String> sliceQuery =
				HFactory.createSliceQuery(m_keyspace, StringSerializer.get(), StringSerializer.get(),
						StringSerializer.get());

		sliceQuery.setColumnFamily(CF_STRING_INDEX);
		sliceQuery.setKey(ROW_KEY_TAG_VALUES);

		ColumnSliceIterator<String, String, String> columnIterator =
				new ColumnSliceIterator<String, String, String>(sliceQuery, "", (String)null, false, m_rowReadSize);

		List<String> ret = new ArrayList<String>();

		while (columnIterator.hasNext())
			ret.add(columnIterator.next().getName());

		return (ret);
	}

	@Override
	protected List<TaggedDataPoints> queryDatabase(DatastoreMetricQuery query, CachedSearchResult cachedSearchResult)
	{
		List<DataPointsRowKey> rowKeys = getKeysForQuery(query);


		QueryRunner qRunner = new QueryRunner(m_keyspace, CF_DATA_POINTS, rowKeys,
				query.getStartTime(), query.getEndTime(), cachedSearchResult, m_rowReadSize);

		try
		{
			qRunner.runQuery();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}

		return cachedSearchResult.getRows();
	}

	/*package*/ List<DataPointsRowKey> getKeysForQuery(DatastoreMetricQuery query)
	{
		List<DataPointsRowKey> ret = new ArrayList<DataPointsRowKey>();

		SliceQuery<String, DataPointsRowKey, String> sliceQuery =
				HFactory.createSliceQuery(m_keyspace, StringSerializer.get(),
						DATA_POINTS_ROW_KEY_SERIALIZER, StringSerializer.get());

		DataPointsRowKey startKey = new DataPointsRowKey(query.getName(),
				calculateRowTime(query.getStartTime()));

		/*
		Adding 1 to the end time ensures we get all the keys that have end time and
		have tags in the key.
		 */
		DataPointsRowKey endKey = new DataPointsRowKey(query.getName(),
				calculateRowTime(query.getEndTime()) + 1);


		sliceQuery.setColumnFamily(CF_ROW_KEY_INDEX)
				.setKey(query.getName());

		ColumnSliceIterator<String, DataPointsRowKey, String> iterator =
				new ColumnSliceIterator<String, DataPointsRowKey, String>(sliceQuery,
						startKey, endKey, false, 1024);

		Map<String, String> filterTags = query.getTags();
		outer: while (iterator.hasNext())
		{
			DataPointsRowKey rowKey = iterator.next().getName();

			Map<String, String> keyTags = rowKey.getTags();
			for (String tag : filterTags.keySet())
			{
				String value = keyTags.get(tag);
				if (!filterTags.get(tag).equals(value))
					continue outer; //Don't want this key
			}

			ret.add(rowKey);
		}

		if (logger.isDebugEnabled())
			logger.debug("Querying the database using " + ret.size() + " keys");

		return (ret);
	}

	private long calculateRowTime(long timestamp)
	{
		return (timestamp - (timestamp % m_rowWidth));
	}


	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.core.datastore.DatastoreMXBean#getDataStoreURL()
	 */
	@Override
	public String getDataStoreURL() {
		return m_cluster.describeClusterName();
	}
}
