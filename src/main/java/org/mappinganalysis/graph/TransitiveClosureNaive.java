package org.mappinganalysis.graph;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.functions.ExcludeInputDataJoinFunction;

import java.util.HashSet;
import java.util.Set;

/**
 * from org.apache.flink.examples.java.graph.TransitiveClosureNaive - working for DataSet as input.
 *
 * input is assumed as directed edges, undirected edges are created automatically
 * result is filtered: no loops
 */
public class TransitiveClosureNaive {

  public static DataSet<Tuple2<Long, Long>> compute(DataSet<Tuple2<Long, Long>> edges, int maxIterations) {
    DataSet<Tuple2<Long, Long>> edgesReversed = edges.project(1, 0);
    DataSet<Tuple2<Long, Long>> edgesJoined = edges.union(edgesReversed);

    IterativeDataSet<Tuple2<Long,Long>> paths = edgesJoined.iterate(maxIterations);

    DataSet<Tuple2<Long,Long>> nextPaths = paths
        .join(edgesJoined)
        .where(1)
        .equalTo(0)
        .with(new JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
          @Override
          public Tuple2<Long, Long> join(Tuple2<Long, Long> left, Tuple2<Long, Long> right) throws Exception {
            return new Tuple2<>(left.f0, right.f1);
          }
        }).withForwardedFieldsFirst("0").withForwardedFieldsSecond("1")
        .union(paths)
        .groupBy(0, 1)
        .reduceGroup(new GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
          @Override
          public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long>> out) throws Exception {
            out.collect(values.iterator().next());
          }
        }).withForwardedFields("0;1");

    DataSet<Tuple2<Long,Long>> newPaths = paths
        .coGroup(nextPaths)
        .where(0).equalTo(0)
        .with(new CoGroupFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
          Set<Tuple2<Long,Long>> prevSet = new HashSet<>();
          @Override
          public void coGroup(Iterable<Tuple2<Long, Long>> prevPaths, Iterable<Tuple2<Long, Long>> nextPaths, Collector<Tuple2<Long, Long>> out) throws Exception {
            for (Tuple2<Long,Long> prev : prevPaths) {
              prevSet.add(prev);
            }
            for (Tuple2<Long,Long> next: nextPaths) {
              if (!prevSet.contains(next)) {
                out.collect(next);
              }
            }
          }
        }).withForwardedFieldsFirst("0").withForwardedFieldsSecond("0");

    return paths.closeWith(nextPaths, newPaths);
  }

  /**
   * Retrieve all new edges for all clusters. Only one edge per direction, no loops.
   * @param inputData input data set
   * @param tmpResult temp result
   * @return cleansed data set of edges
   */
  public static DataSet<Tuple2<Long, Long>> restrictToNewEdges(DataSet<Tuple2<Long, Long>> inputData,
                                                               DataSet<Tuple2<Long, Long>> tmpResult) {
    return tmpResult
        .filter(new FilterFunction<Tuple2<Long, Long>>() {
          @Override
          public boolean filter(Tuple2<Long, Long> tuple) throws Exception {
            return (long) tuple.f0 != tuple.f1;
          }
        })
        .leftOuterJoin(inputData)
        .where(0, 1).equalTo(0, 1)
        .with(new ExcludeInputDataJoinFunction())
        .leftOuterJoin(inputData)
        .where(0, 1).equalTo(1, 0)
        .with(new ExcludeInputDataJoinFunction())
        .map(new MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
          @Override
          public Tuple2<Long, Long> map(Tuple2<Long, Long> tuple) throws Exception {
            return tuple.f0 < tuple.f1 ? tuple : tuple.swap();
          }
        })
        .distinct();
  }
}
