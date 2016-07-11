package org.mappinganalysis.graph;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.junit.Test;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;
import org.mappinganalysis.utils.functions.keyselector.CcIdKeySelector;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClusterComputationTest {

  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  @Test
  public void computeMissingEdgesTest() throws Exception {
    Graph<Long, NullValue, NullValue> graph = createTestGraph();
    DataSet<Vertex<Long, ObjectMap>> inputVertices = arrangeVertices(graph);
    DataSet<Edge<Long, NullValue>> allEdges
        = GraphUtils.computeComponentEdges(inputVertices, new CcIdKeySelector());

    assertEquals(9, allEdges.count());

    DataSet<Edge<Long, NullValue>> newEdges
        = GraphUtils.restrictToNewEdges(graph.getEdges(), allEdges);
    assertEquals(1, newEdges.count());
    assertTrue(newEdges.collect().contains(new Edge<>(5681L, 5984L, NullValue.getInstance())));
  }

  @Test
  public void getAllEdgesInGraphTest() throws Exception {
    final Graph<Long, NullValue, NullValue> graph = createTestGraph();
    final DataSet<Vertex<Long, ObjectMap>> inputVertices = arrangeVertices(graph);
    final DataSet<Edge<Long, NullValue>> tooMuchEdges
        = GraphUtils.computeComponentEdges(inputVertices, new CcIdKeySelector());

    assertEquals(9, tooMuchEdges.count());

    final DataSet<Edge<Long, NullValue>> simpleAllEdges
        = GraphUtils.getDistinctSimpleEdges(tooMuchEdges);

    assertEquals(3, simpleAllEdges.count());
    assertTrue(simpleAllEdges.collect().contains(new Edge<>(5681L, 5984L, NullValue.getInstance())));

    final DataSet<Edge<Long, NullValue>> secondMethod
        = GraphUtils.computeComponentEdges(inputVertices, new CcIdKeySelector());
    assertEquals(3, secondMethod.count());
  }


  private DataSet<Vertex<Long, ObjectMap>> arrangeVertices(Graph<Long, NullValue, NullValue> graph) {
    return graph
        .getVertices()
        .map(new MapFunction<Vertex<Long, NullValue>, Vertex<Long, ObjectMap>>() {
          @Override
          public Vertex<Long, ObjectMap> map(Vertex<Long, NullValue> vertex) throws Exception {
            ObjectMap prop = new ObjectMap();
            prop.put(Utils.CC_ID, 5680L);

            return new Vertex<>(vertex.getId(), prop);
          }
        });
  }

  private Graph<Long, NullValue, NullValue> createTestGraph() {
    List<Edge<Long, NullValue>> edgeList = Lists.newArrayList();
    edgeList.add(new Edge<>(5680L, 5681L, NullValue.getInstance()));
    edgeList.add(new Edge<>(5680L, 5984L, NullValue.getInstance()));
    return Graph.fromCollection(edgeList, env);
  }
}
