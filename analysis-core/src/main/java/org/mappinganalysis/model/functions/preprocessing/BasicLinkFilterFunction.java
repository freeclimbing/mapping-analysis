package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.graph.GraphUtils;
import org.mappinganalysis.graph.LinkFilterFunction;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.Preprocessing;

/**
 * Actual implementation for basic link filter.
 *
 * preprocessing: currently in use simple 1:n removal
 * TODO grouping based on ccId still used for creating independent blocks, how to avoid?
 */
public class BasicLinkFilterFunction extends LinkFilterFunction {
  private Boolean removeIsolatedVertices;
  private ExecutionEnvironment env;

  public BasicLinkFilterFunction(Boolean removeIsolatedVertices, ExecutionEnvironment env) {
    this.removeIsolatedVertices = removeIsolatedVertices;
    this.env = env;
  }

  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(Graph<Long, ObjectMap, ObjectMap> graph) throws Exception {
    graph = GraphUtils.addCcIdsToGraph(graph, env);

    // EdgeSourceSimTuple(edge src, edge trg, vertex ont, neighbor ont, EdgeSim)
    DataSet<EdgeSourceSimTuple> neighborTuples = graph
        .groupReduceOnNeighbors(new SecondNeighborOntologyFunction(), EdgeDirection.OUT);

    DataSet<Tuple2<Long, Long>> edgeTuples = neighborTuples.groupBy(0)
        .sortGroup(5, Order.DESCENDING)
        .sortGroup(1, Order.ASCENDING)
        .sortGroup(2, Order.ASCENDING)
        .reduceGroup(new LinkSelectionWithCcIdFunction());

    DataSet<Edge<Long, ObjectMap>> newEdges = edgeTuples.join(graph.getEdges())
        .where(0, 1)
        .equalTo(0, 1)
        .with((tuple, edge) -> edge)
        .returns(new TypeHint<Edge<Long, ObjectMap>>() {
        });

    DataSet<Vertex<Long, ObjectMap>> resultVertices;
    if (removeIsolatedVertices) {
      resultVertices = Preprocessing.deleteVerticesWithoutAnyEdges(
          graph.getVertices(),
          newEdges.<Tuple2<Long, Long>>project(0, 1));
    } else {
      resultVertices = graph.getVertices();
    }

    return Graph.fromDataSet(resultVertices, newEdges, env);
  }
}
