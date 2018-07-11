package org.mappinganalysis.graph;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.graph.utils.EdgeComputationStrategy;
import org.mappinganalysis.graph.utils.EdgeComputationOnVerticesForKeySelector;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.incremental.BlockingKeySelector;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EdgeComputationOnVerticesForKeySelectorTest {
  private static final Logger LOG = Logger.getLogger(EdgeComputationOnVerticesForKeySelectorTest.class);

  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  @Test
  public void SimpleEdgesCreatorTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    String graphPath = EdgeComputationOnVerticesForKeySelectorTest
        .class.getResource("/data/typeGroupBy/").getFile();
    DataSet<Vertex<Long, ObjectMap>> vertices =
        new JSONDataSource(graphPath, true, env)
        .getVertices();

    DataSet<Edge<Long, NullValue>> resultEdges = vertices
        .runOperation(new EdgeComputationOnVerticesForKeySelector(
            new CcIdKeySelector(),
            EdgeComputationStrategy.SIMPLE));

    assertEquals(8, resultEdges.count());
  }

  @Test
  public void StringKeySelectorTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    String graphPath = EdgeComputationOnVerticesForKeySelectorTest
        .class.getResource("/data/typeGroupBy/").getFile();
    DataSet<Vertex<Long, ObjectMap>> vertices =
        new JSONDataSource(graphPath, true, env)
            .getVertices()
            .map(vertex -> {
              vertex.getValue().setMode(Constants.GEO);
              vertex.getValue().setBlockingKey(
                  BlockingStrategy.STANDARD_BLOCKING,
                  4);
              return vertex;
            })
            .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});

    DataSet<Edge<Long, NullValue>> resultEdges = vertices
        .runOperation(new EdgeComputationOnVerticesForKeySelector(
            new BlockingKeySelector()));

    int clusterOneEdgeCount = 0;
    int clusterTwoEdgeCount = 0;
    for (Edge<Long, NullValue> edge : resultEdges.collect()) {
      if (edge.getSource() == 122L
          || edge.getSource() == 123L
          || edge.getSource() == 1181L) {
        clusterOneEdgeCount++;
      } else if (edge.getSource() == 617158L
          || edge.getSource() == 617159L
          || edge.getSource() == 1022884L) {
        clusterTwoEdgeCount++;
      }
    }
    assertEquals(6, clusterOneEdgeCount);
    assertEquals(6, clusterTwoEdgeCount);
  }

  /**
   * all edges (1,1), (2,2), (2,1), (1,2) not needed.
   */
  @Test
  public void allEdgesCreatorTest() throws Exception {
    env = TestBase.setupLocalEnvironment();
    Graph<Long, NullValue, NullValue> graph = createTestGraph();
    DataSet<Vertex<Long, ObjectMap>> inputVertices = prepareVertices(graph);

    final DataSet<Edge<Long, NullValue>> distinctEdges = inputVertices
        .runOperation(new EdgeComputationOnVerticesForKeySelector(new CcIdKeySelector()));

    int count = 0;
    for (Edge<Long, NullValue> edge : distinctEdges.collect()) {
      count++;
      if (edge.getSource() == 5680L) {
        assertTrue(edge.getTarget() == 5984L || edge.getTarget() == 5681L);
      } else if (edge.getSource() == 5681L) {
        assertTrue(edge.getTarget() == 5984L);
      }
    }
    assertEquals(3, count);
  }

  /**
   * Add some meta data to vertices for test.
   */
  private DataSet<Vertex<Long, ObjectMap>> prepareVertices(
      Graph<Long, NullValue, NullValue> graph) {
    return graph
        .getVertices()
        .map(vertex -> {
          ObjectMap prop = new ObjectMap(Constants.GEO);
          prop.put(Constants.CC_ID, 5680L);

          return new Vertex<>(vertex.getId(), prop);
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});
  }

  private Graph<Long, NullValue, NullValue> createTestGraph() {
    List<Edge<Long, NullValue>> edgeList = Lists.newArrayList();
    edgeList.add(new Edge<>(5680L, 5681L, NullValue.getInstance()));
    edgeList.add(new Edge<>(5680L, 5984L, NullValue.getInstance()));

    return Graph.fromCollection(edgeList, env);
  }
}
