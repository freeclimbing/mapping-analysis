package org.mappinganalysis;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.simsort.AggSimValueEdgeMapFunction;
import org.mappinganalysis.model.functions.simsort.SimSort;
import org.mappinganalysis.model.functions.simsort.TripletToEdgeMapFunction;
import org.mappinganalysis.model.functions.typegroupby.TypeGroupBy;
import org.mappinganalysis.utils.Utils;
import org.s1ck.gdl.GDLHandler;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * basic test class
 */
public class MappingAnalysisExampleTest {
  private static final Logger LOG = Logger.getLogger(MappingAnalysisExample.class);
  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  private static final String simple = "g[" +
      "(v1 {compType = \"no_type_available\", hashCc = 12L})" +
      "(v2 {compType = \"Mountain\", hashCc = 23L})" +
      "(v3 {compType = \"Settlement\", hashCc = 42L})" +
      "(v4 {compType = \"Settlement\", hashCc = 42L})]" +
      "(v1)-[e1:sameAs {aggSimValue = .9D}]->(v2)" +
      "(v1)-[e2:sameAs {aggSimValue = .4D}]->(v3)" +
      "(v1)-[e3:sameAs {aggSimValue = .7D}]->(v4)";

  private static final String sortSimple = "g[" +
      "(v1 {typeIntern = \"Settlement\", label = \"bajaur\", lat = 34.683333D, lon = 71.5D, hashCc = 23L})" +
      "(v2 {typeIntern = \"Settlement\", label = \"Bajaur Agency\", lat = 34.6833D, lon = 71.5D, hashCc = 23L})" +
      "(v3 {typeIntern = \"AdministrativeRegion\", lat = 34.8333333D, lon = 71.5D, hashCc = 23L})" +
      "(v4 {label = \"Bajaur (Pakistan)\", lat = 34.8333D, lon = 71.5D, hashCc = 23L})]" +
      "(v4)-[e1:sameAs]->(v1)" +
      "(v4)-[e2:sameAs]->(v2)" +
      "(v3)-[e3:sameAs]->(v3)";

  private static final String tripleUnknown = "g[" +
      "(v1 {compType = \"Settlement\", hashCc = 12L})" +
      "(v2 {compType = \"no_type_available\", hashCc = 21L})" +
      "(v3 {compType = \"no_type_available\", hashCc = 33L})" +
      "(v4 {compType = \"no_type_available\", hashCc = 42L})" +
      "(v5 {compType = \"School\", hashCc = 51L})]" +
      "(v1)-[e1:sameAs {aggSimValue = .9D}]->(v2)" +
      "(v2)-[e2:sameAs {aggSimValue = .6D}]->(v3)" +
      "(v3)-[e3:sameAs {aggSimValue = .7D}]->(v4)" +
      "(v4)-[e4:sameAs {aggSimValue = .4D}]->(v5)";

  @Test
  public void simSortTest() throws Exception {
    Utils.PRE_CLUSTER_STRATEGY = Utils.CMD_COMBINED;
    Utils.IGNORE_MISSING_PROPERTIES = true;
    GDLHandler firstHandler = new GDLHandler.Builder().buildFromString(sortSimple);
    Graph<Long, ObjectMap, ObjectMap> firstGraph = createTestGraph(firstHandler);

    firstGraph = SimSort.prepare(firstGraph, env);
    firstGraph = SimSort.execute(firstGraph, 100, 0.65);

    for (Vertex<Long, ObjectMap> vertex : firstGraph.getVertices().collect()) {
      LOG.info(vertex);
    }
    for (Edge<Long, ObjectMap> edge : firstGraph.getEdges().collect()) {
      LOG.info(edge);
    }
  }

  @Test
  public void typeGroupByTest() throws Exception {
    GDLHandler firstHandler = new GDLHandler.Builder().buildFromString(simple);
    Graph<Long, ObjectMap, ObjectMap> firstGraph = createTestGraph(firstHandler);

    firstGraph = new TypeGroupBy().execute(firstGraph, 100);

    for (Vertex<Long, ObjectMap> vertex : firstGraph.getVertices().collect()) {
      ObjectMap value = vertex.getValue();
      if (vertex.getId() == 1 || vertex.getId() == 2) {
        assertTrue((value.containsKey(Utils.TMP_TYPE) && value.get(Utils.TMP_TYPE).equals("Mountain"))
            || value.get(Utils.COMP_TYPE).equals("Mountain"));
        assertEquals(value.get(Utils.HASH_CC), 23L);
      } else {
        assertEquals(value.get(Utils.HASH_CC), 42L);
        assertTrue(value.get(Utils.COMP_TYPE).equals("Settlement"));
      }
    }

//    LOG.info("#### Second example ###");
    GDLHandler secondHandler = new GDLHandler.Builder().buildFromString(tripleUnknown);
    Graph<Long, ObjectMap, ObjectMap> secondGraph = createTestGraph(secondHandler);

    secondGraph = new TypeGroupBy().execute(secondGraph, 100);

    for (Vertex<Long, ObjectMap> vertex : secondGraph.getVertices().collect()) {
      ObjectMap value = vertex.getValue();
      if (vertex.getId() == 5) {
        assertTrue(value.get(Utils.COMP_TYPE).equals("School"));
        assertEquals(value.get(Utils.HASH_CC), 51L);
      } else {
        assertEquals(value.get(Utils.HASH_CC), 12L);
        assertTrue((value.containsKey(Utils.TMP_TYPE) && value.get(Utils.TMP_TYPE).equals("Settlement"))
        || value.get(Utils.COMP_TYPE).equals("Settlement"));
      }
    }
  }

  private Graph<Long, ObjectMap, ObjectMap> createTestGraph(GDLHandler handler) {
    List<Edge<Long, ObjectMap>> edgeList = Lists.newArrayList();
    List<Vertex<Long, ObjectMap>> vertexList = Lists.newArrayList();

    // create Gelly edges and vertices -> graph
    for (org.s1ck.gdl.model.Vertex v : handler.getVertices()) {
      vertexList.add(new Vertex<>(v.getId(), new ObjectMap(v.getProperties())));
    }
    for (org.s1ck.gdl.model.Edge edge : handler.getEdges()) {
      edgeList.add(new Edge<>(edge.getSourceVertexId(),
          edge.getTargetVertexId(),
          new ObjectMap(edge.getProperties())));
    }

    return Graph.fromCollection(vertexList, edgeList, env);
  }

  /*
   * deprecated
   */
  private Graph<Long, ObjectMap, NullValue> createTestGraph() {
    List<Edge<Long, NullValue>> edgeList = Lists.newArrayList();
    edgeList.add(new Edge<>(5680L, 5681L, NullValue.getInstance()));
    edgeList.add(new Edge<>(5680L, 5984L, NullValue.getInstance()));
    Graph<Long, NullValue, NullValue> tmpGraph = Graph.fromCollection(edgeList, env);

    final DataSet<Vertex<Long, ObjectMap>> baseVertices = tmpGraph.getVertices()
        .map(new MapFunction<Vertex<Long, NullValue>, Vertex<Long, ObjectMap>>() {
          @Override
          public Vertex<Long, ObjectMap> map(Vertex<Long, NullValue> vertex) throws Exception {
            ObjectMap prop = new ObjectMap();
            prop.put(Utils.LABEL, "foo");
            return new Vertex<>(vertex.getId(), prop);
          }
        });

    return Graph.fromDataSet(baseVertices, tmpGraph.getEdges(), env);
  }
}