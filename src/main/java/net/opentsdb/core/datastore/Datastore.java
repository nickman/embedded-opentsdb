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
package net.opentsdb.core.datastore;


import static com.google.common.base.Preconditions.checkNotNull;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPointSet;
import net.opentsdb.core.aggregator.Aggregator;
import net.opentsdb.core.aggregator.AvgAggregator;
import net.opentsdb.core.aggregator.MaxAggregator;
import net.opentsdb.core.aggregator.MinAggregator;
import net.opentsdb.core.aggregator.NoneAggregator;
import net.opentsdb.core.aggregator.StdAggregator;
import net.opentsdb.core.aggregator.SumAggregator;
import net.opentsdb.core.exception.DatastoreException;
import net.opentsdb.core.exception.UnknownAggregator;
import net.opentsdb.util.TournamentTree;

public abstract class Datastore
{
	private final Map<String, Aggregator> aggregators = new HashMap<String, Aggregator>();
	MessageDigest messageDigest;
	protected final AtomicBoolean started = new AtomicBoolean(false);
	protected final BlockingQueue<DataPointSet> dataPointQueue = new ArrayBlockingQueue<DataPointSet>(1000, false);
	
	/** The enqueued count */
	protected final AtomicLong enqueued = new AtomicLong(0);
	/** The dequeued count */
	protected final AtomicLong dequeued = new AtomicLong(0);
	/** The drop count */
	protected final AtomicLong dropped = new AtomicLong(0);
	
	
	/**
	 * Indicates if this data store is started
	 * @return true if this data store is started, false otherwise
	 */
	public boolean isStarted() {
		return started.get();
	}
	
	public long getEnqueuedCount() {
		return enqueued.get();
	}
	
	public long getDequeuedCount() {
		return dequeued.get();
	}
	
	public long getDroppedCount() {
		return dropped.get();
	}
	
	public int getQueueSize() {
		return dataPointQueue.size();
	}

	public void queueDataPoints(DataPointSet dps) {
		if(dps!=null) {
			if(dataPointQueue.offer(dps)) {
				enqueued.incrementAndGet();
			} else {
				dropped.incrementAndGet();
			}
		}
	}

	public void putDataPoints(Collection<DataPointSet> dpColl) {
		/* No Op */
	}
	
	protected Datastore() throws DatastoreException
	{
		try
		{
			messageDigest = MessageDigest.getInstance("MD5");
		}
		catch (NoSuchAlgorithmException e)
		{
			throw new DatastoreException(e);
		}

		aggregators.put("sum", new SumAggregator());
		aggregators.put("min", new MinAggregator());
		aggregators.put("max", new MaxAggregator());
		aggregators.put("avg", new AvgAggregator());
		aggregators.put("dev", new StdAggregator());
		aggregators.put("none", new NoneAggregator());
	}

	/**
	 * Close the datastore
	 */
	public abstract void close() throws InterruptedException, DatastoreException;

	public abstract void putDataPoints(DataPointSet dps) throws DatastoreException;

	public abstract Iterable<String> getMetricNames() throws DatastoreException;


	public abstract Iterable<String> getTagNames() throws DatastoreException;

	public abstract Iterable<String> getTagValues() throws DatastoreException;

	public List<DataPointGroup> query(QueryMetric metric) throws DatastoreException
	{
		checkNotNull(metric);

		CachedSearchResult cachedResults = null;

		try
		{
			String cacheFilename = calculateFilenameHash(metric);
			String tempFile = System.getProperty("java.io.tmpdir") + "/" + cacheFilename;

			if (metric.getCacheTime() > 0)
			{
				cachedResults = CachedSearchResult.openCachedSearchResult(metric.getName(), tempFile, metric.getCacheTime());
			}

			if (cachedResults == null)
			{
				cachedResults = CachedSearchResult.createCachedSearchResult(metric.getName(), tempFile);
			}
		}
		catch (Exception e)
		{
			throw new DatastoreException(e);
		}

		List<DataPointGroup> aggregatedResults = new ArrayList<DataPointGroup>();

		List<DataPointGroup> queryResults = groupBy(metric.getName(), queryDatabase(metric, cachedResults), metric.getGroupBy());

		for (DataPointGroup dataPointGroup : queryResults)
		{
			Aggregator aggregator = aggregators.get(metric.getAggregator());
			if (aggregator == null)
			{
				throw new UnknownAggregator(metric.getAggregator());
			}

			aggregatedResults.add(aggregator.aggregate(dataPointGroup));
		}

		return aggregatedResults;
	}

	private List<DataPointGroup> groupBy(String metricName, List<TaggedDataPoints> dataPointsList, String groupByTag)
	{
		List<DataPointGroup> ret;

		if (groupByTag != null)
		{
			Map<String, TournamentTreeDataGroup> groups = new HashMap<String, TournamentTreeDataGroup>();

			for (TaggedDataPoints taggedDataPoints : dataPointsList)
			{
				//Todo: Add code to datastore implementations to filter by the group by tag

				String tagValue = taggedDataPoints.getTags().get(groupByTag);

				if (tagValue == null)
					continue;

				TournamentTreeDataGroup tree = groups.get(tagValue);
				if (tree == null)
				{
					tree = new TournamentTreeDataGroup(metricName);
					groups.put(tagValue, tree);
				}

				tree.addIterator(taggedDataPoints);
			}

			ret = new ArrayList<DataPointGroup>(groups.values());
		}
		else
		{
			ret = new ArrayList<DataPointGroup>();
			ret.add(new TournamentTreeDataGroup(metricName, dataPointsList));
		}

		return ret;
	}

	protected abstract List<TaggedDataPoints> queryDatabase(DatastoreMetricQuery query, CachedSearchResult cachedSearchResult) throws DatastoreException;

	private class TournamentTreeDataGroup extends DataPointGroup
	{
		private TournamentTree<DataPoint> tree;
		//We keep this list so we can close the iterators
		private List<TaggedDataPoints> taggedDataPointsList = new ArrayList<TaggedDataPoints>();

		public TournamentTreeDataGroup(String name)
		{
			super(name);

			tree = new TournamentTree<DataPoint>(new DataPointComparator());
		}

		public TournamentTreeDataGroup(String name, List<TaggedDataPoints> listTaggedDataPoints)
		{
			this(name);

			for (TaggedDataPoints dataPoints : listTaggedDataPoints)
			{
				addIterator(dataPoints);
			}
		}

		@Override
		public void close()
		{
			for (TaggedDataPoints taggedDataPoints : taggedDataPointsList)
			{
				taggedDataPoints.close();
			}
		}

		@Override
		public boolean hasNext()
		{
			return tree.hasNext();
		}

		@Override
		public DataPoint next()
		{
			DataPoint ret = tree.nextElement();

			return ret;
		}

		public void addIterator(TaggedDataPoints taggedDataPoints)
		{
			tree.addIterator(taggedDataPoints);
			addTags(taggedDataPoints.getTags());
			taggedDataPointsList.add(taggedDataPoints);
		}
	}

	private class DataPointComparator implements Comparator<DataPoint>
	{
		@Override
		public int compare(DataPoint point1, DataPoint point2)
		{
			return point1.compareTo(point2);
		}
	}

	private String calculateFilenameHash(QueryMetric metric) throws NoSuchAlgorithmException, UnsupportedEncodingException
	{
		StringBuilder builder = new StringBuilder();
		builder.append(metric.getName());
		builder.append(metric.getStartTime());
		builder.append(metric.getEndTime());
		builder.append(metric.getAggregator());

		SortedMap<String, String> tags = metric.getTags();
		for (String key : metric.getTags().keySet())
		{
			builder.append(key).append("=").append(tags.get(key));
		}

		byte[] digest = messageDigest.digest(builder.toString().getBytes("UTF-8"));

		return new BigInteger(1, digest).toString(16);
	}
}
