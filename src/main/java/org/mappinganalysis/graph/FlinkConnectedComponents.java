package org.mappinganalysis.graph;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.util.HashSet;

/**
 * Compute Flink Connected Components.
 */
public class FlinkConnectedComponents {
  private static final Logger LOG = Logger.getLogger(FlinkConnectedComponents.class);

  /**
   * used in first version of component check, soon deprecated
   * @param vertexSet set of vertices
   * @param edgeSet set of edges
   * @param maxIterations max iterations for cc
   * @return flink dataset
   */
  public static DataSet<Tuple2<Long, Long>> compute(HashSet<Integer> vertexSet, HashSet<Tuple2<Integer, Integer>> edgeSet,
                                                    int maxIterations, ExecutionEnvironment env) throws Exception {
    DataSet<Long> vertices = env.fromCollection(vertexSet).map(new LongMapper());
    DataSet<Tuple2<Long, Long>> edges = env.fromCollection(edgeSet)
        .map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Long, Long>>() {
          @Override
          public Tuple2<Long, Long> map(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
            return new Tuple2<>((long) integerIntegerTuple2.f0, (long) integerIntegerTuple2.f1);
          }
        });
//        .flatMap(new UndirectedEdge());
    return compute(vertices, edges, maxIterations);
  }

  /**
   * Compute connected components with Flink
   * @param vertices set of vertices
   * @param inEdges set of edges
   * @param maxIterations max iterations for cc
   * @return flink dataset
   */
  public static DataSet<Tuple2<Long, Long>> compute(DataSet<Long> vertices, DataSet<Tuple2<Long, Long>> inEdges,
                                                    int maxIterations) throws Exception {
    LOG.info("Started connected components computation...");
    DataSet<Tuple2<Long, Long>> edges = inEdges.flatMap(new UndirectedEdge());
    // assign the initial component IDs (equal to the vertex ID)
    DataSet<Tuple2<Long, Long>> verticesWithInitialId = vertices.map(new DuplicateValue<Long>());

    // open a delta iteration
    DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
        verticesWithInitialId.iterateDelta(verticesWithInitialId, maxIterations, 0);

    // apply the step logic:
    DataSet<Tuple2<Long, Long>> changes = iteration.getWorkset()
        // join with the edges
        .join(edges).where(0).equalTo(0).with(new NeighborWithComponentIDJoin())
            // select the minimum neighbor component ID
        .groupBy(0).aggregate(Aggregations.MIN, 1)
            // update if the component ID of the candidate is smaller
        .join(iteration.getSolutionSet()).where(0).equalTo(0)
        .flatMap(new ComponentIdFilter());

    // close the delta iteration (delta and new workset are identical)
    DataSet<Tuple2<Long, Long>> result = iteration.closeWith(changes, changes);
    LOG.info("Done.");

    return result;
  }

  private static final class DuplicateValue<T> implements MapFunction<T, Tuple2<T, T>> {

    @Override
    public Tuple2<T, T> map(T vertex) {
      return new Tuple2<>(vertex, vertex);
    }
  }

  private static final class UndirectedEdge
      implements FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
    protected Tuple2<Long, Long> invertedEdge = new Tuple2<>();

    @Override
    public void flatMap(Tuple2<Long, Long> edge, Collector<Tuple2<Long, Long>> out) {
      invertedEdge.f0 = edge.f1;
      invertedEdge.f1 = edge.f0;
      out.collect(edge);
      out.collect(invertedEdge);
    }
  }

  private static final class NeighborWithComponentIDJoin
      implements JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

    @Override
    public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexWithComponent, Tuple2<Long, Long> edge) {
      return new Tuple2<>(edge.f1, vertexWithComponent.f1);
    }
  }

  private static final class ComponentIdFilter
      implements FlatMapFunction<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>,
      Tuple2<Long, Long>> {

    @Override
    public void flatMap(Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> value,
                        Collector<Tuple2<Long, Long>> out) {
      if (value.f0.f1 < value.f1.f1) {
        out.collect(value.f0);
      }
    }
  }

  private static class LongMapper implements MapFunction<Integer, Long> {
    @Override
    public Long map(Integer value) throws Exception {
      return (long) value;
    }
  }
}
