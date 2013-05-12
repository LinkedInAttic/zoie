package proj.zoie.impl.indexing.internal;
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Comparator;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Similarity;

import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieHealth;
import proj.zoie.api.indexing.IndexingEventListener;
import proj.zoie.api.indexing.OptimizeScheduler;
import proj.zoie.api.indexing.OptimizeScheduler.OptimizeType;
import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.impl.indexing.internal.SearchIndexManager.Status;

public class DiskLuceneIndexDataLoader<R extends IndexReader> extends LuceneIndexDataLoader<R>
{

	private long _lastTimeOptimized;
	private static final Logger log = Logger.getLogger(DiskLuceneIndexDataLoader.class);
  private final ScheduledFuture<?> _scheduledPurge;

  private volatile OptimizeScheduler _optScheduler;

	public DiskLuceneIndexDataLoader(Analyzer analyzer,
                                   Similarity similarity,
                                   SearchIndexManager<R> idxMgr,
                                   Comparator<String> comparator,
                                   Queue<IndexingEventListener> lsnrList,
                                   Filter purgeFilter,
                                   ScheduledExecutorService executor,
                                   final int numDeletionsBeforeOptimize,
                                   long purgePeriod) {
		super(analyzer, similarity, idxMgr,comparator,lsnrList, purgeFilter, executor, numDeletionsBeforeOptimize,
        purgePeriod);
		_lastTimeOptimized=System.currentTimeMillis();
		_optimizeMonitor = new Object();

    if(executor != null && numDeletionsBeforeOptimize > 0 && purgePeriod > 0) {
      purgeFunction();

      _scheduledPurge = executor.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          purgeFunction();
        }
      }, purgePeriod, purgePeriod, TimeUnit.MILLISECONDS);
    } else {
      _scheduledPurge = null;
    }
  }

  private synchronized void purgeFunction() {
    try {
      int numDocsPurged = purgeDocuments();
      getSearchIndex().commitDeletes();
      int numDeletions = getNumDeletions();
      if (numDeletions + numDocsPurged > _numDeletionsBeforeOptimize) {
        try {
          optimize(1);
          BaseSearchIndex<R> searchIndex = getSearchIndex();
          searchIndex.refresh(true);

        } catch (IOException e) {
          log.error("Could not optimize search index", e);
        }
      }
    } catch (Throwable th) {
      log.error("Error during purge job!!!", th);
    }
  }

  public void setOptimizeScheduler(OptimizeScheduler scheduler){
		_optScheduler = scheduler;
	}
	
	public OptimizeScheduler getOptimizeScheduler(){
		return _optScheduler;
	}

	@Override
	protected BaseSearchIndex<R> getSearchIndex() {
		return _idxMgr.getDiskIndex();
	}

    @Override
    protected void propagateDeletes(LongSet delDocs) throws IOException
    {
      // do nothing
    }
    
    @Override
    protected void commitPropagatedDeletes() throws IOException
    {
      // do nothing
    }

	@Override
	public void consume(Collection<DataEvent<ZoieIndexable>> events) throws ZoieException
	{
		// updates the in memory status before and after the work
		synchronized(_optimizeMonitor)
		{
		  try
		  {
		    _idxMgr.setDiskIndexerStatus(Status.Working);
		    OptimizeType optType = _optScheduler.getScheduledOptimizeType();
		    _idxMgr.setPartialExpunge(optType == OptimizeType.PARTIAL);
		    try
		    {
		      super.consume(events);
		    }
		    finally
		    {
		      _optScheduler.finished();
		      _idxMgr.setPartialExpunge(false);
		    }
		    
		    if(optType == OptimizeType.FULL)
		    {
	          try
	          {
	            expungeDeletes();
	          }
	          catch(IOException ioe)
	          {
	            ZoieHealth.setFatal();
	            throw new ZoieException(ioe.getMessage(),ioe);
	          }
	          finally
	          {
	            _optScheduler.finished();
	          }
		    }
		  }
		  finally
		  {
            _idxMgr.setDiskIndexerStatus(Status.Sleep);		    
		  }
		}
	}
	
	@Override
    public void loadFromIndex(RAMSearchIndex<R> ramIndex) throws ZoieException
    {
	  
      synchronized(_optimizeMonitor)
      {
        try
        {
          _idxMgr.setDiskIndexerStatus(Status.Working);
          
          OptimizeType optType = _optScheduler.getScheduledOptimizeType();
          _idxMgr.setPartialExpunge(optType == OptimizeType.PARTIAL);
          try
          {
            super.loadFromIndex(ramIndex);
          }
          finally
          {
            _optScheduler.finished();
            _idxMgr.setPartialExpunge(false);
          }
          
          if(optType == OptimizeType.FULL)
          {
            try
            {
              expungeDeletes();
            }
            catch(IOException ioe)
            {
              ZoieHealth.setFatal();
              throw new ZoieException(ioe.getMessage(),ioe);
            }
            finally
            {
              _optScheduler.finished();
            }
          }
        }
        finally
        {
          _idxMgr.setDiskIndexerStatus(Status.Sleep);         
        }
      }
    }

	public long getLastTimeOptimized()
	{
		return _lastTimeOptimized;
	}
	
	public long exportSnapshot(WritableByteChannel channel) throws IOException
	{
	  DiskSearchIndex<R> idx = (DiskSearchIndex<R>)getSearchIndex();
	  if(idx != null)
	  {
	    DiskIndexSnapshot snapshot = null;
        
	    try
	    {
	      synchronized(_optimizeMonitor) // prevent index updates while taking a snapshot
	      {
	        snapshot = idx.getSnapshot();
	      }
	      
	      return (snapshot != null ?  snapshot.writeTo(channel) : 0);
	    }
	    finally
	    {
	      if(snapshot != null) snapshot.close();
	    }
	  }
	  return 0;
	}
	
	public void importSnapshot(ReadableByteChannel channel) throws IOException
	{
      DiskSearchIndex<R> idx = (DiskSearchIndex<R>)getSearchIndex();
      if(idx != null)
      {
        synchronized(_optimizeMonitor) // prevent index updates while taking a snapshot
        {
	      _idxMgr.purgeIndex();
	      idx.importSnapshot(channel);
	      _idxMgr.refreshDiskReader();
	    }
	  }
	}

  @Override
  public void close() {
    if (_optScheduler != null)
    {
      log.info("shutting down zoie's OptimizeScheduler ...");
      _optScheduler.shutdown();
    }

    if(_scheduledPurge != null)
    {
      _scheduledPurge.cancel(false);
    }
  }
}
