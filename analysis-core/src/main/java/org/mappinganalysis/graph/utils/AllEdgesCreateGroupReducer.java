package org.mappinganalysis.graph.utils;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class AllEdgesCreateGroupReducer<T extends Tuple>
    implements GroupReduceFunction<T, Edge<Long, NullValue>> {
  @Override
  public void reduce(Iterable<T> values, Collector<Edge<Long, NullValue>> out) throws Exception {
    HashSet<T> rightVertices = Sets.newHashSet(values);
    HashSet<T> leftVertices = Sets.newHashSet(rightVertices);

    for (T leftVertex : leftVertices) {
      rightVertices.remove(leftVertex);
      for (T rightVertex : rightVertices) {
          if ((long) leftVertex.getField(0) < (long) rightVertex.getField(0)) {
            out.collect(new Edge<>(leftVertex.getField(0),
                rightVertex.getField(0),
                NullValue.getInstance()));
          } else {
            out.collect(new Edge<>(rightVertex.getField(0),
                leftVertex.getField(0),
                NullValue.getInstance()));
          }
      }
    }
  }
}
