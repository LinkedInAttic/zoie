package proj.zoie.test.data;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;

import java.util.Map;

public class PurgeDataInterpreter implements ZoieIndexableInterpreter<Map<String, String>>
{
  @Override
  public ZoieIndexable convertAndInterpret(final Map<String, String> src) {
    return new ZoieIndexable() {
      @Override
      public long getUID() {
        return Long.parseLong(src.get("id"));
      }

      @Override
      public boolean isDeleted() {
        return false;
      }

      @Override
      public boolean isSkip() {
        return false;
      }

      @Override
      public IndexingReq[] buildIndexingReqs() {
        Document document = new Document();
        for (Map.Entry<String, String> entry : src.entrySet()) {
          document.add(new Field(entry.getKey(), entry.getValue(), Store.YES, Index.NOT_ANALYZED));
        }
        IndexingReq indexingReq = new IndexingReq(document);
        return new IndexingReq[]{indexingReq};
      }

      @Override
      public boolean isStorable() {
        return false;
      }

      @Override
      public byte[] getStoreValue() {
        return src.toString().getBytes();
      }
    };
  }
}
