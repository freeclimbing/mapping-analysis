package org.mappinganalysis.graph;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.CcIdKeySelector;
import org.mappinganalysis.model.functions.ExcludeInputDataJoinFunction;

import java.util.HashSet;

public class ClusterComputation {

  public static DataSet<Edge<Long, NullValue>> computeComponentEdges(
      DataSet<Vertex<Long, ObjectMap>>  vertices) {
    return vertices.coGroup(vertices)
        .where(new CcIdKeySelector())
        .equalTo(new CcIdKeySelector())
        .with(new CoGroupFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>, Edge<Long, NullValue>>() {
          @Override
          public void coGroup(Iterable<Vertex<Long, ObjectMap>> left,
                              Iterable<Vertex<Long, ObjectMap>> right,
                              Collector<Edge<Long, NullValue>> collector) throws Exception {
            HashSet<Vertex<Long, ObjectMap>> rightSet = Sets.newHashSet(right);
            for (Vertex<Long, ObjectMap> vertexLeft : left) {
              for (Vertex<Long, ObjectMap> vertexRight : rightSet) {
                collector.collect(new Edge<>(vertexLeft.getId(),
                    vertexRight.getId(), NullValue.getInstance()));
              }
            }
          }
        });
  }

  public static DataSet<Edge<Long, NullValue>> restrictToNewEdges(DataSet<Edge<Long, NullValue>> input,
                                                                  DataSet<Edge<Long, NullValue>> tmpResult) {
    return tmpResult
        .filter(new FilterFunction<Edge<Long, NullValue>>() {
          @Override
          public boolean filter(Edge<Long, NullValue> edge) throws Exception {
            return (long) edge.getSource() != edge.getTarget();
          }
        })
        .leftOuterJoin(input)
        .where(0, 1).equalTo(0, 1)
        .with(new ExcludeInputJoinFunction())
        .leftOuterJoin(input)
        .where(0, 1).equalTo(1, 0)
        .with(new ExcludeInputJoinFunction())
        .map(new MapFunction<Edge<Long, NullValue>, Edge<Long, NullValue>>() {
          @Override
          public Edge<Long, NullValue> map(Edge<Long, NullValue> edge) throws Exception {
            return edge.getSource() < edge.getTarget() ? edge : edge.reverse();
          }
        })
        .distinct();
  }

  /**
   * Retrieve all new edges for all clusters. Only one edge per direction, no loops.
   * @param inputData input data set
   * @param tmpResult temp result
   * @return cleansed data set of edges
   */
  public static DataSet<Tuple2<Long, Long>> restrictToNewTuples(DataSet<Tuple2<Long, Long>> inputData,
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

  private static class ExcludeInputJoinFunction implements FlatJoinFunction<Edge<Long, NullValue>,
      Edge<Long, NullValue>, Edge<Long, NullValue>> {
    @Override
    public void join(Edge<Long, NullValue> left, Edge<Long, NullValue> right,
                     Collector<Edge<Long, NullValue>> collector) throws Exception {
      if (right == null) {
        collector.collect(left);
      }
    }
  }
}
