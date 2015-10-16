package org.mappinganalysis.graph;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * Compute Flink Connected Components.
 */
public class FlinkConnectedComponents {

  public DataSet<Tuple2<Integer, Integer>> compute(HashSet<Integer> vertexSet,
                                                   HashSet<Tuple2<Integer, Integer>> edgeSet,
                                                   int maxIterations) {
    ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

    DataSource<Integer> vertices = env.fromCollection(vertexSet);
    DataSet<Tuple2<Integer, Integer>> edges = env.fromCollection(edgeSet).flatMap(new UndirectEdge());

    // assign the initial component IDs (equal to the vertex ID)
    DataSet<Tuple2<Integer, Integer>> verticesWithInitialId = vertices.map(new DuplicateValue<Integer>());

    // open a delta iteration
    DeltaIteration<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> iteration =
        verticesWithInitialId.iterateDelta(verticesWithInitialId, maxIterations, 0);

    // apply the step logic:
    DataSet<Tuple2<Integer, Integer>> changes = iteration.getWorkset()
        // join with the edges
        .join(edges).where(0).equalTo(0).with(new NeighborWithComponentIDJoin())
            // select the minimum neighbor component ID
        .groupBy(0).aggregate(Aggregations.MIN, 1)
            // update if the component ID of the candidate is smaller
        .join(iteration.getSolutionSet()).where(0).equalTo(0)
        .flatMap(new ComponentIdFilter());

// close the delta iteration (delta and new workset are identical)
    return iteration.closeWith(changes, changes);
  }

  private static final class DuplicateValue<T> implements MapFunction<T, Tuple2<T, T>> {

    @Override
    public Tuple2<T, T> map(T vertex) {
      return new Tuple2<>(vertex, vertex);
    }
  }

  private static final class UndirectEdge
      implements FlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
    protected Tuple2<Integer, Integer> invertedEdge = new Tuple2<>();

    @Override
    public void flatMap(Tuple2<Integer, Integer> edge, Collector<Tuple2<Integer, Integer>> out) {
      invertedEdge.f0 = edge.f1;
      invertedEdge.f1 = edge.f0;
      out.collect(edge);
      out.collect(invertedEdge);
    }
  }

  private static final class NeighborWithComponentIDJoin
      implements JoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

    @Override
    public Tuple2<Integer, Integer> join(Tuple2<Integer, Integer> vertexWithComponent, Tuple2<Integer, Integer> edge) {
      return new Tuple2<>(edge.f1, vertexWithComponent.f1);
    }
  }

  private static final class ComponentIdFilter
      implements FlatMapFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>,
      Tuple2<Integer, Integer>> {

    @Override
    public void flatMap(Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> value,
                        Collector<Tuple2<Integer, Integer>> out) {
      if (value.f0.f1 < value.f1.f1) {
        out.collect(value.f0);
      }
    }
  }

}
