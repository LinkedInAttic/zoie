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
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Similarity;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieHealth;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.IndexingEventListener;
import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexable.IndexingReq;

public abstract class LuceneIndexDataLoader<R extends IndexReader> implements DataConsumer<ZoieIndexable>
{
	private static final Logger log = Logger.getLogger(LuceneIndexDataLoader.class);
	protected final Analyzer _analyzer;
	protected final Similarity _similarity;
	protected final SearchIndexManager<R> _idxMgr;
	protected final Comparator<String> _versionComparator;
	protected final ScheduledExecutorService _executor;
  protected final Filter _purgeFilter;
  protected Object _optimizeMonitor = new Object();
  private final Queue<IndexingEventListener> _lsnrList;

  protected final int _numDeletionsBeforeOptimize;

	protected LuceneIndexDataLoader(Analyzer analyzer,
                                  Similarity similarity,
                                  SearchIndexManager<R> idxMgr,
                                  Comparator<String> versionComparator,
                                  Queue<IndexingEventListener> lsnrList,
                                  Filter purgeFilter,
                                  ScheduledExecutorService executor,
                                  int numDeletionsBeforeOptimize,
                                  long purgePeriod) {
		_analyzer = analyzer;
		_similarity = similarity;
		_idxMgr=idxMgr;
		_versionComparator = versionComparator;
		_purgeFilter = purgeFilter;
		_lsnrList = lsnrList;
    _numDeletionsBeforeOptimize = numDeletionsBeforeOptimize;
    _executor = executor;
	}
	
	protected abstract BaseSearchIndex<R> getSearchIndex();
	
  protected abstract void propagateDeletes(LongSet delDocs) throws IOException;
  protected abstract void commitPropagatedDeletes() throws IOException;

  protected final int getNumDeletions() {
    BaseSearchIndex<R> idx = getSearchIndex();
    ZoieIndexReader<R> reader = null;
    try {
      if(idx != null) {
        synchronized (idx) {
          reader = idx.openIndexReader();
          if (reader != null) {
            reader.incZoieRef();
            return reader.numDeletedDocs();
          }
        }
      }
      return 0;
    } catch (IOException e) {
      log.error("Error opening reader to check num deleted docs");
      return 0;
    } finally {
      if(reader != null) {
        reader.decZoieRef();
      }
    }
  }

  protected final int purgeDocuments() {
    synchronized (_optimizeMonitor) {
      if (_purgeFilter != null) {
        BaseSearchIndex<R> idx = getSearchIndex();
        IndexReader writeReader = null;

        log.info("purging docs started...");
        int count = 0;
        long start = System.currentTimeMillis();

        ZoieIndexReader<R> reader = null;
        try {
          synchronized (idx) {
            idx.refresh(false);
            reader = idx.openIndexReader();
            if (reader != null)
              reader.incZoieRef();
          }

          writeReader = idx.openIndexReaderForDelete();

          DocIdSetIterator iter = _purgeFilter.getDocIdSet(reader).iterator();

          int doc;
          while ((doc = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            count++;
            writeReader.deleteDocument(doc);
          }

        } catch (Throwable th) {
          log.error("problem creating purge filter: " + th.getMessage(), th);
        } finally {
          if (reader != null) {
            reader.decZoieRef();
          }
          if (writeReader != null) {
            try {
              writeReader.close();
            } catch (IOException ioe) {
              ZoieHealth.setFatal();
              log.error(ioe.getMessage(), ioe);
            }
          }
        }

        long end = System.currentTimeMillis();
        log.info("purging docs completed in " + (end - start) + "ms");
        log.info("total docs purged: " + count);
        return count;
      }
      return 0;
    }
  }

  public void optimize(int numSegs) throws IOException
  {
    long t0 = System.currentTimeMillis();
    if (numSegs<=1) numSegs = 1;
    log.info("optmizing, numSegs: "+numSegs+" ...");

    // we should optimize
    synchronized(_optimizeMonitor)
    {
      BaseSearchIndex<R> idx=getSearchIndex();
      IndexWriter writer=null;
      try
      {
        writer=idx.openIndexWriter(_analyzer, _similarity);
        writer.forceMerge(numSegs, true);
        writer.commit();
      }
      finally
      {
        if (writer!=null)
        {
          idx.closeIndexWriter();
        }
      }
      _idxMgr.refreshDiskReader();
    }
    log.info("index optimized in " + (System.currentTimeMillis() - t0) +"ms");
  }

  public void expungeDeletes() throws IOException
  {
    log.info("expunging deletes...");
    synchronized(_optimizeMonitor)
    {
      BaseSearchIndex<R> idx=getSearchIndex();
      IndexWriter writer=null;
      try
      {
        writer=idx.openIndexWriter(_analyzer, _similarity);
        writer.expungeDeletes(true);
      }
      finally
      {
        if (writer!=null)
        {
          idx.closeIndexWriter();
        }
      }
      _idxMgr.refreshDiskReader();
    }
    log.info("deletes expunged");
  }


  /**
	 * @Precondition incoming events sorted by version number
	 * <br>every event in the events collection must be non-null
	 * 
	 * @see proj.zoie.api.DataConsumer#consume(java.util.Collection)
	 * 
	 */
	public void consume(Collection<DataEvent<ZoieIndexable>> events) throws ZoieException {
		
        if (events == null)
			return;

        int eventCount = events.size();
        if (eventCount==0){
        	return;
        }
		BaseSearchIndex<R> idx = getSearchIndex();

		if (idx==null){
			throw new ZoieException("trying to consume to null index");
		}
		Long2ObjectMap<List<IndexingReq>> addList = new Long2ObjectOpenHashMap<List<IndexingReq>>();
		String version = idx.getVersion();		// current version

		LongSet delSet =new LongOpenHashSet();
		
		try {
		  for(DataEvent<ZoieIndexable> evt : events)
		  {
		    if (evt == null) continue;
    		    //version = Math.max(version, evt.getVersion());
		        version = version == null ? evt.getVersion() : (_versionComparator.compare(version,evt.getVersion()) < 0 ? evt.getVersion() : version);
		        
		        if (evt instanceof MarkerDataEvent) continue;
    		    // interpret and get get the indexable instance
    		    ZoieIndexable indexable = evt.getData();
    		    if (indexable == null || indexable.isSkip())
    		      continue;
    
    		    long uid = indexable.getUID();
    		    delSet.add(uid);
    		    addList.remove(uid);
				if (!(indexable.isDeleted() || evt.isDelete())) // update event
				{
					try {
  				  IndexingReq[] reqs = indexable.buildIndexingReqs();
  					for (IndexingReq req : reqs) {
  						if (req != null) // if doc is provided, interpret as
  											// a delete, e.g. update with
  											// nothing
  						{
  							Document doc = req.getDocument();
  							if (doc!=null){							 
  							  ZoieSegmentReader.fillDocumentID(doc, uid);
  							  if (indexable.isStorable()){
  							    byte[] bytes = indexable.getStoreValue();
  							    if (bytes!=null){
  							      doc.add(new Field(AbstractZoieIndexable.DOCUMENT_STORE_FIELD,bytes));
  							    }
  							  }
  							}
  							// add to the insert list
  							List<IndexingReq> docList = addList.get(uid);
  							if (docList == null) {
  								docList = new LinkedList<IndexingReq>();
  								addList.put(uid, docList);
  							}
  							docList.add(req);
  						}
  					}
  				} catch (Exception ex) {
  				  log.error("Couldn't index the event with uid - " + uid, ex);
  				}
				}
				// hao: we do not need the following few lines
				//else {
					//addList.remove(uid);
				//}
			}

			List<IndexingReq> docList = new ArrayList<IndexingReq>(addList.size());
			for (List<IndexingReq> tmpList : addList.values()) {
				docList.addAll(tmpList);
			}

      purgeDocuments();
      idx.updateIndex(delSet, docList, _analyzer,_similarity);
      propagateDeletes(delSet);
			synchronized(_idxMgr)
			{
         idx.refresh(false);
         commitPropagatedDeletes();
			}
		} catch (IOException ioe) {
      ZoieHealth.setFatal();
			log.error("Problem indexing batch: " + ioe.getMessage(), ioe);
		} finally {
			try {
				if (idx != null) {
          idx.setVersion(version); // update the version of the
					idx.incrementEventCount(eventCount);
												// index
				}
			} catch (Exception e) // catch all exceptions, or it would screw
									// up jobs framework
			{
				log.warn(e.getMessage());
			} finally {
				if (idx instanceof DiskSearchIndex<?>) {
					log.info("disk indexing requests flushed.");
				}
			}
		}
	}
	
    public void loadFromIndex(RAMSearchIndex<R> ramIndex) throws ZoieException
    {
      try
      {
        log.info("Starting disk index partial optimization");
        long start = System.nanoTime();

        // hao: get disk search idx, 
        BaseSearchIndex<R> idx = getSearchIndex();
        //hao: merge the realyOnly ram idx with the disk idx
        idx.loadFromIndex(ramIndex);
//      duplicate clearDeletes, delDoc may change for realtime delete after loadFromIndex()
//        idx.clearDeletes(); // clear old deletes as deletes are written to the lucene index
        // hao: update the disk idx reader
        idx.refresh(false); // load the index reader
        purgeDocuments();
        idx.markDeletes(ramIndex.getDelDocs()); // inherit deletes
        idx.commitDeletes();
        idx.incrementEventCount(ramIndex.getEventsHandled());
        
        //Map<String, String> commitData = idx.getCommitData();
        //System.out.println("disk vesion from the commit data" + commitData);  
        
        //V newVersion = idx.getVersion().compareTo(ramIndex.getVersion()) < 0 ? ramIndex.getVersion(): idx.getVersion();
        String newVersion = idx.getVersion() == null ? ramIndex.getVersion() : (_versionComparator.compare(idx.getVersion(), ramIndex.getVersion()) < 0 ? ramIndex.getVersion(): idx.getVersion());
        idx.setVersion(newVersion);      
        //System.out.println("disk verson from the signature" + newVersion.toString());        
               
        //idx.setVersion(Math.max(idx.getVersion(), ramIndex.getVersion()));

        log.info("Finished disk index partial optimization in " + (System.nanoTime() - start) + "ns");
      }
      catch(IOException ioe)
      {
        ZoieHealth.setFatal();
        log.error("Problem copying segments: " + ioe.getMessage(), ioe);
        throw new ZoieException(ioe);
      }
    }
 

  /**
   * @return the version number of the search index.
   */
  public String getVersion()
  {
    BaseSearchIndex<R> idx = getSearchIndex();
    String version = null;
    if (idx != null) version = idx.getVersion();
    return version;
  }

	/**
   * @return the version comparator.
   */
	public Comparator<String> getVersionComparator() {
    return _versionComparator;
  }

  public abstract void close();
}
