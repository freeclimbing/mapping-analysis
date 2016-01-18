package org.mappinganalysis.model.functions;

import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

public class ExtractEdgeValuesFlatMapFunction implements FlatMapFunction<Edge<Long, ObjectMap>, Tuple4<Long, String, Double, Integer>> {
  @Override
  public void flatMap(Edge<Long, ObjectMap> edge, Collector<Tuple4<Long, String, Double, Integer>> collector) throws Exception {
    for (String s : edge.getValue().keySet()) {
      collector.collect(new Tuple4<>(edge.getSource(), Utils.AGG_PREFIX.concat(s),
          Doubles.tryParse(edge.getValue().get(s).toString()), 1));
      collector.collect(new Tuple4<>(edge.getTarget(), Utils.AGG_PREFIX.concat(s),
          Doubles.tryParse(edge.getValue().get(s).toString()), 1));
    }
  }
}
