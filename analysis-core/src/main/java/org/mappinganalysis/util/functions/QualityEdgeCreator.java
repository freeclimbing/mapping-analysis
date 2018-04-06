package org.mappinganalysis.util.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;

import java.util.Set;

public class QualityEdgeCreator
    implements FlatMapFunction<Vertex<Long, ObjectMap>, Tuple2<Long, Long>> {
  @Override
  public void flatMap(Vertex<Long, ObjectMap> cluster, Collector<Tuple2<Long, Long>> out) throws Exception {
    Set<Long> firstSide = cluster.getValue().getVerticesList();
    Set<Long> secondSide = Sets.newHashSet(firstSide);
    for (Long first : firstSide) {
      secondSide.remove(first);
      for (Long second : secondSide) {
        if (first < second) {
          out.collect(new Tuple2<>(first, second));
        } else {
          out.collect(new Tuple2<>(second, first));
        }
      }
    }
  }
}
