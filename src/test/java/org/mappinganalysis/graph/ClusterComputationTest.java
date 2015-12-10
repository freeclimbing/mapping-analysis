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

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClusterComputationTest {

  @Test
  public void computeMissingEdgesTest() throws Exception {
    List<Edge<Long, NullValue>> edgeList = Lists.newArrayList();
    edgeList.add(new Edge<>(5680L, 5681L, NullValue.getInstance()));
    edgeList.add(new Edge<>(5680L, 5984L, NullValue.getInstance()));
    Graph<Long, NullValue, NullValue> graph = Graph.fromCollection(edgeList, ExecutionEnvironment.createLocalEnvironment());

    DataSet<Vertex<Long, ObjectMap>> inputVertices = graph
        .getVertices()
        .map(new MapFunction<Vertex<Long, NullValue>, Vertex<Long, ObjectMap>>() {
          @Override
          public Vertex<Long, ObjectMap> map(Vertex<Long, NullValue> vertex) throws Exception {
            ObjectMap prop = new ObjectMap();
            prop.put(Utils.CC_ID, 5680L);

            return new Vertex<>(vertex.getId(), prop);
          }
        });

    DataSet<Edge<Long, NullValue>> allEdges
        = ClusterComputation.computeComponentEdges(inputVertices);

    assertEquals(9, allEdges.count());

    DataSet<Edge<Long, NullValue>> newEdges
        = ClusterComputation.restrictToNewEdges(graph.getEdges(), allEdges);
    assertEquals(1, newEdges.count());
    assertTrue(newEdges.collect().contains(new Edge<>(5681L, 5984L, NullValue.getInstance())));
  }
}
