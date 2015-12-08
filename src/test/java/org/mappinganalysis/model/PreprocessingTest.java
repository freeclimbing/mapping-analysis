package org.mappinganalysis.model;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.*;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.junit.Test;
import org.mappinganalysis.MappingAnalysisExample;
import org.mappinganalysis.utils.Utils;

import java.util.Map;

public class PreprocessingTest {

  @Test
  public void testApplyLinkFilterStrategy() throws Exception {

  }

  @Test
  public void testApplyTypePreprocessing() throws Exception {
    ExecutionEnvironment environment = ExecutionEnvironment.createLocalEnvironment();
    Graph<Long, FlinkVertex, NullValue> graph = MappingAnalysisExample.getInputGraph(Utils.GEO_FULL_NAME, environment);

    graph = Preprocessing.applyLinkFilterStrategy(graph, environment);
    graph = Preprocessing.applyTypePreprocessing(graph, environment);

    graph.getVertices().map(new MapFunction<Vertex<Long, FlinkVertex>, Vertex<Long, String>>() {
      @Override
      public Vertex<Long, String> map(Vertex<Long, FlinkVertex> vertex) throws Exception {
        String result = "";
        Map<String, Object> properties = vertex.getValue().getProperties();
        if (properties.containsKey(Utils.TYPE_INTERN)) {
          result = result.concat("intern: ").concat(properties.get(Utils.TYPE_INTERN).toString());
        }
        if (properties.containsKey(Utils.TYPE)) {
          result = result.concat(" ### type: " ).concat(properties.get(Utils.TYPE).toString());
        }

        return new Vertex<>(vertex.getId(), result);
      }
    }).print();
  }
}