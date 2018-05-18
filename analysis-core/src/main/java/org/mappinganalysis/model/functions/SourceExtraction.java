package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;

/**
 * Extract all (distinct) dataSource values from a given graph.
 * @param <EV> support all edge values, not needed anyway
 */
public class SourceExtraction<EV>
    implements GraphAlgorithm<Long, ObjectMap, EV, DataSet<String>> {
  @Override
  public DataSet<String> run(Graph<Long, ObjectMap, EV> input) throws Exception {
    return input.getVertices()
        .flatMap(new FlatMapFunction<Vertex<Long, ObjectMap>, String>() {
          @Override
          public void flatMap(Vertex<Long, ObjectMap> value, Collector<String> out) throws Exception {
            for (String source : value.getValue().getDataSourcesList()) {
              out.collect(source);
            }
          }
        })
        .distinct();
  }
}
