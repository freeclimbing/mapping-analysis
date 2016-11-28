package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.graph.LinkFilterFunction;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.Preprocessing;
import org.mappinganalysis.model.functions.decomposition.FinalOneToManyRemovalFunction;

/**
 * After simple 1:n eliminating, still 1:n can reoccur after creating transitive closure
 * in components. The best candidate of the 1:n vertices remains in the vertex dataset,
 * others are removed.
 */
public class ClusteringLinkFilterFunction extends LinkFilterFunction {
  private ExecutionEnvironment env;

  public ClusteringLinkFilterFunction(ExecutionEnvironment env) {
    this.env = env;
  }
  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(Graph<Long, ObjectMap, ObjectMap> graph) throws Exception {
      DataSet<Tuple3<Long, String, Double>> oneToManyCandidates = graph
        .groupReduceOnNeighbors(new FinalOneToManyRemovalFunction(), EdgeDirection.ALL);

    DataSet<Vertex<Long, ObjectMap>> bestCandidates = oneToManyCandidates.groupBy(1)
        .max(2).andMin(0)
        .map(tuple -> new Tuple2<>(tuple.f1, tuple.f2)) // string, double
        .returns(new TypeHint<Tuple2<String, Double>>() {})
        .join(oneToManyCandidates)
        .where(0, 1)
        .equalTo(1, 2)
        .with((Tuple2<String, Double> left,
               Tuple3<Long, String, Double> right,
               Collector<Tuple1<Long>> out) -> new Tuple1<>(right.f0))
        .returns(new TypeHint<Tuple1<Long>>() {})
        .join(graph.getVertices())
        .where(0)
        .equalTo(0)
        .with((tuple, vertex) -> vertex)
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});

    DataSet<Vertex<Long, ObjectMap>> resultVertices = graph.getVertices()
        .leftOuterJoin(oneToManyCandidates)
        .where(0)
        .equalTo(0)
        .with((Vertex<Long, ObjectMap> left,
               Tuple3<Long, String, Double> right,
               Collector<Vertex<Long, ObjectMap>> out) -> {
          if (right == null) {
            out.collect(left);
          }
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {})
        .union(bestCandidates);

    DataSet<Edge<Long, ObjectMap>> resultEdges = Preprocessing.deleteEdgesWithoutSourceOrTarget(
        graph.getEdges(),
        resultVertices);

    return Graph.fromDataSet(resultVertices, resultEdges, env);  }
}
