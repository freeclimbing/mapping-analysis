package org.mappinganalysis.model.functions.decomposition.typegroupby;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.preprocessing.TypeOverlapCcCreator;
import org.mappinganalysis.util.config.Config;

import static org.junit.Assert.assertEquals;

/**
 * TypeGroupByTest - ALL edges in graph are required to work properly.
 */
public class TypeGroupByTest {
  private static final Logger LOG = Logger.getLogger(TypeGroupByTest.class);
  private static ExecutionEnvironment env = TestBase.setupLocalEnvironment();

  @Test
  public void newTgbTest() throws Exception {
    String graphPath = TypeGroupByTest.class.getResource("/data/typeGroupBy/").getFile();

    // little workaround needed because hash in HashCcIdOverlappingFunction may change for different runs
    // --> but resulting grouping is always correct
    boolean isKarl = false;
    boolean isLake1 = false;
    boolean isLake2 = false;
    boolean isFake = false;
    long resultKarl = 0; // all "same" type
    long resultLake1 = 0; // once lake, once settlement
    long resultLake2 = 0;
    long resultFake = 0; // all no_type -> same hash

    Config config = new Config(DataDomain.GEOGRAPHY, env);

    DataSet<Vertex<Long, ObjectMap>> vertices =
        new JSONDataSource(graphPath, true, env)
        .getGraph()
        .run(new TypeOverlapCcCreator(config))
//        .getVertices().print();
        .run(new TypeGroupBy(env))
        .getVertices();
//            .print();

    for (Vertex<Long, ObjectMap> vertex : vertices.collect()) {
      if (vertex.getId() == 1375705L || vertex.getId() == 617158L
          || vertex.getId() == 617159L || vertex.getId() == 1022884L) {
        if (!isKarl) {
          resultKarl = vertex.getValue().getHashCcId();
          isKarl = true;
        } else {
          assertEquals(resultKarl, vertex.getValue().getHashCcId().longValue());
        }
      } else if (vertex.getId() == 2060L || vertex.getId() == 123L) {
        if (!isLake1) {
          resultLake1 = vertex.getValue().getHashCcId();
          isLake1 = true;
        } else {
          assertEquals(resultLake1, vertex.getValue().getHashCcId().longValue());
        }
      } else if (vertex.getId() == 122L || vertex.getId() == 1181L) {
        if (!isLake2) {
          resultLake2 = vertex.getValue().getHashCcId();
          isLake2 = true;
        } else {
          assertEquals(resultLake2, vertex.getValue().getHashCcId().longValue());
        }
      } else if (vertex.getId() == 1L || vertex.getId() == 2L || vertex.getId() == 3L) {
        if (!isFake) {
          resultFake = vertex.getValue().getHashCcId();
          isFake = true;
        } else {
          assertEquals(resultFake, vertex.getValue().getHashCcId().longValue());
        }
      }
    }
    vertices.print();
  }
}
