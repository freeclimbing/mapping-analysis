package org.mappinganalysis.model;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.junit.Test;
import org.mappinganalysis.utils.Utils;

import java.util.List;
import java.util.Map;

public class PreprocessingTest {
  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  @Test
  public void testApplyTypePreprocessing() throws Exception {
    List<Edge<Long, NullValue>> edgeList = Lists.newArrayList();
    edgeList.add(new Edge<>(5680L, 5681L, NullValue.getInstance()));
    edgeList.add(new Edge<>(5680L, 5984L, NullValue.getInstance()));
    Graph<Long, NullValue, NullValue> tmpGraph = Graph.fromCollection(edgeList, env);

    final DataSet<Vertex<Long, ObjectMap>> baseVertices = tmpGraph.getVertices()
        .map(new MapFunction<Vertex<Long, NullValue>, Vertex<Long, ObjectMap>>() {
          @Override
          public Vertex<Long, ObjectMap> map(Vertex<Long, NullValue> vertex) throws Exception {
            ObjectMap prop = new ObjectMap();
            prop.put(Utils.TYPE_INTERN, "foo");
            return new Vertex<>(vertex.getId(), prop);
          }
        });

    Graph<Long, ObjectMap, NullValue> graph = Graph.fromDataSet(baseVertices, tmpGraph.getEdges(), env);
//    graph = Preprocessing.applyLinkFilterStrategy(graph, env, true); TODO
    graph = Preprocessing.applyTypeToInternalTypeMapping(graph, env);

    graph.getVertices().map(new MapFunction<Vertex<Long, ObjectMap>, Vertex<Long, String>>() {
      @Override
      public Vertex<Long, String> map(Vertex<Long, ObjectMap> vertex) throws Exception {
        String result = "";
        Map<String, Object> properties = vertex.getValue();
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

//  @Test
//  public void testApplyTypePreprocessing() throws Exception {
//    Graph<Long, ObjectMap, NullValue> graph = MappingAnalysisExample.getInputGraph(Utils.GEO_FULL_NAME);
//
//    graph = Preprocessing.applyLinkFilterStrategy(graph, env, true);
//    graph = Preprocessing.applyTypeToInternalTypeMapping(graph, env);
//
//    graph.getVertices().map(new MapFunction<Vertex<Long, ObjectMap>, Vertex<Long, String>>() {
//      @Override
//      public Vertex<Long, String> map(Vertex<Long, ObjectMap> vertex) throws Exception {
//        String result = "";
//        Map<String, Object> properties = vertex.getValue();
//        if (properties.containsKey(Utils.TYPE_INTERN)) {
//          result = result.concat("intern: ").concat(properties.get(Utils.TYPE_INTERN).toString());
//        }
//        if (properties.containsKey(Utils.TYPE)) {
//          result = result.concat(" ### type: " ).concat(properties.get(Utils.TYPE).toString());
//        }
//
//        return new Vertex<>(vertex.getId(), result);
//      }
//    }).print();
//  }
}