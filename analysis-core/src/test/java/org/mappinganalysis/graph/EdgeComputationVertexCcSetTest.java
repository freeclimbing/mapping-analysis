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
import org.mappinganalysis.graph.utils.EdgeComputationVertexCcSet;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.functions.LeftMinusRightSideJoinFunction;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EdgeComputationVertexCcSetTest {
  private static final Logger LOG = Logger.getLogger(EdgeComputationVertexCcSetTest.class);

  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  @Test
  public void SimpleEdgesCreatorTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    String graphPath = EdgeComputationVertexCcSetTest
        .class.getResource("/data/typeGroupBy/").getFile();
    DataSet<Vertex<Long, ObjectMap>> vertices =
        new JSONDataSource(graphPath, true, env)
        .getVertices();

    DataSet<Edge<Long, NullValue>> resultEdges = vertices
        .runOperation(new EdgeComputationVertexCcSet(
            new CcIdKeySelector(),
            EdgeComputationStrategy.SIMPLE));

    assertEquals(8, resultEdges.count());
  }

  /**
   * all edges (1,1), (2,2), (2,1), (1,2) not needed.
   */
  @Test
  public void allEdgesCreatorTest() throws Exception {
    env = TestBase.setupLocalEnvironment();
    Graph<Long, NullValue, NullValue> graph = createTestGraph();
    DataSet<Vertex<Long, ObjectMap>> inputVertices = prepareVertices(graph);

    // why this test? useless?
//    DataSet<Edge<Long, NullValue>> allEdges = inputVertices
//        .runOperation(new EdgeComputationVertexCcSet(new CcIdKeySelector(),
//            EdgeComputationStrategy.ALL,
//            false));
//
//    allEdges.print();
//    assertEquals(9, allEdges.count());
//
//    DataSet<Edge<Long, NullValue>> newEdges
//        = restrictToNewEdges(graph.getEdges(), allEdges);
//    assertEquals(1, newEdges.count());
//    assertTrue(newEdges.collect()
//        .contains(new Edge<>(5681L, 5984L, NullValue.getInstance())));


    final DataSet<Edge<Long, NullValue>> distinctEdges = inputVertices
        .runOperation(new EdgeComputationVertexCcSet(new CcIdKeySelector()));

    distinctEdges.print();
    assertEquals(3, distinctEdges.count());
    assertTrue(distinctEdges.collect()
        .contains(new Edge<>(5681L, 5984L, NullValue.getInstance())));
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

  /**
   * Restrict given set to edges which are not in the input edges set.
   * @param input edges in this dataset should no longer be in the result set
   * @param processEdges remove edges from input edge dataset from these and return
   *
   * Used for tests.
   */
  @Deprecated
  private DataSet<Edge<Long, NullValue>> restrictToNewEdges(
      DataSet<Edge<Long, NullValue>> input,
      DataSet<Edge<Long, NullValue>> processEdges) {
    return processEdges
        .filter(edge -> edge.getSource().longValue() != edge.getTarget())
        .leftOuterJoin(input)
        .where(0, 1)
        .equalTo(0, 1)
        .with(new LeftMinusRightSideJoinFunction<>())
        .leftOuterJoin(input)
        .where(0, 1)
        .equalTo(1, 0)
        .with(new LeftMinusRightSideJoinFunction<>())
        .map(edge -> edge.getSource() < edge.getTarget() ? edge : edge.reverse())
        .returns(new TypeHint<Edge<Long, NullValue>>() {})
        .distinct();
  }
}
