package org.mappinganalysis.model.functions;

import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

public class ExtractEdgeSimValueFlatMapFunction implements FlatMapFunction<Edge<Long, ObjectMap>, Tuple3<Long, Double, Integer>> {
  @Override
  public void flatMap(Edge<Long, ObjectMap> edge, Collector<Tuple3<Long, Double, Integer>> collector) throws Exception {
    for (String s : edge.getValue().keySet()) {
      collector.collect(new Tuple3<>(edge.getSource(),
          Doubles.tryParse(edge.getValue().get(Utils.AGGREGATED_SIM_VALUE).toString()), 1));
      collector.collect(new Tuple3<>(edge.getTarget(),
          Doubles.tryParse(edge.getValue().get(Utils.AGGREGATED_SIM_VALUE).toString()), 1));
    }
  }
}
