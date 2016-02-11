package org.mappinganalysis;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.graph.ClusterComputation;
import org.mappinganalysis.graph.FlinkConnectedComponents;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.CcVerticesCreator;
import org.mappinganalysis.model.functions.typegroupby.TypeGroupBy;
import org.mappinganalysis.utils.Utils;
import org.s1ck.gdl.GDLHandler;

import java.util.List;

/**
 * basic test class
 */
public class MappingAnalysisExampleTest {
  private static final Logger LOG = Logger.getLogger(MappingAnalysisExample.class);
  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  @Test
  public void analysisTest() throws Exception {
    GDLHandler handler = new GDLHandler.Builder().buildFromString("g[" +
        "(v1 {typeIntern = \"Settlement\", hashCc = 12L})" +
        "(v2 {typeIntern = \"no_type_available\", hashCc = 21L})" +
        "(v3 {typeIntern = \"no_type_available\", hashCc = 33L})" +
        "(v4 {typeIntern = \"no_type_available\", hashCc = 42L})" +
        "(v5 {typeIntern = \"School\", hashCc = 51L})]" +
        "(v1)-[e1:sameAs {aggSimValue = .9D}]->(v2)" +
        "(v2)-[e2:sameAs {aggSimValue = .6D}]->(v3)" +
        "(v3)-[e3:sameAs {aggSimValue = .7D}]->(v4)" +
        "(v4)-[e4:sameAs {aggSimValue = .4D}]->(v5)");

    List<Edge<Long, ObjectMap>> edgeList = Lists.newArrayList();
    List<Vertex<Long, ObjectMap>> vertexList = Lists.newArrayList();

    for (org.s1ck.gdl.model.Vertex v : handler.getVertices()) {
      LOG.info(v.toString());
      vertexList.add(new Vertex<>(v.getId(), new ObjectMap(v.getProperties())));
    }

    for (org.s1ck.gdl.model.Edge edge : handler.getEdges()) {
      edgeList.add(new Edge<>(edge.getSourceVertexId(),
          edge.getTargetVertexId(),
          new ObjectMap(edge.getProperties())));
    }

    Graph<Long, ObjectMap, ObjectMap> input = Graph.fromCollection(vertexList, edgeList, env);



    Graph<Long, ObjectMap, ObjectMap> iterationGraph = new TypeGroupBy().execute(input, 100);

    for (Vertex<Long, ObjectMap> vertex : iterationGraph.getVertices().collect()) {
      LOG.info(vertex);
    }

    for (Edge<Long, ObjectMap> edge : iterationGraph.getEdges().collect()) {
      LOG.info(edge);
    }
  }

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