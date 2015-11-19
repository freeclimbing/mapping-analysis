package org.mappinganalysis;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.junit.Test;
import org.mappinganalysis.io.JDBCDataLoader;
import org.mappinganalysis.model.FlinkVertex;
import org.mappinganalysis.model.functions.VertexCreator;
import org.s1ck.gdl.GDLHandler;

import static org.junit.Assert.assertEquals;

/**
 * basic test class
 */
public class BasicTest {


  @SuppressWarnings("unchecked")
  protected Graph<Long, FlinkVertex, NullValue> createSimpleGraph() throws Exception {
    LocalEnvironment environment = ExecutionEnvironment.createLocalEnvironment();

    JDBCDataLoader loader = new JDBCDataLoader(environment);
    DataSet<Vertex<Long, FlinkVertex>> vertices = loader.getVertices().filter(new FilterFunction<FlinkVertex>() {
      @Override
      public boolean filter(FlinkVertex vertex) throws Exception {
        return vertex.getId() == 4795 || vertex.getId() == 5680 || vertex.getId() == 5984 || vertex.getId() == 5681;
      }
    })
        .map(new VertexCreator());

    Edge<Long, NullValue> correctEdge1 = new Edge<>(5680L, 5681L, NullValue.getInstance());
    Edge<Long, NullValue> correctEdge2 = new Edge<>(5680L, 5984L, NullValue.getInstance());
    Edge<Long, NullValue> wrongEdge = new Edge<>(5680L, 4795L, NullValue.getInstance());

    DataSet<Edge<Long, NullValue>> edges
        = environment.fromCollection(Sets.newHashSet(correctEdge1, correctEdge2, wrongEdge));
    edges.print();

    return Graph.fromDataSet(vertices, edges, environment);

//    Map<String, Object> properties = Maps.newHashMap();
//    properties.put("label", "Leipzig");
//    properties.put("type", "Settlement");
//    properties.put("ontology", "http://dbpedia.org/");
//    FlinkVertex v1 = new FlinkVertex(1L, properties);
//
//    Map<String, Object> properties2 = Maps.newHashMap();
//    properties2.put("label", "Leipzig, Sachsen");
//    properties2.put("ontology", "http://sws.geonames.org/");
//    FlinkVertex v2 = new FlinkVertex(2L, properties2);
//
//    Map<String, Object> properties3 = Maps.newHashMap();
//    properties3.put("label", "halle");
//    properties3.put("ontology", "http://rdf.freebase.com/");
//    FlinkVertex v3 = new FlinkVertex(3L, properties3);
//
//    Map<String, Object> properties4 = Maps.newHashMap();
//    properties4.put("label", "leipzig");
//    properties4.put("ontology", "http://rdf.freebase.com/");
//    FlinkVertex v4 = new FlinkVertex(4L, properties4);
//
//    List<FlinkVertex> temp = Lists.newArrayList(v1, v2, v3, v4);
//    final DataSet<FlinkVertex> flinkTemp = environment.fromCollection(temp);
//    flinkTemp.print();
//    System.out.println(TypeExtractor.getAllDeclaredFields(v4.getClass()));
//    System.out.println(TypeExtractor.getForObject(flinkTemp.getType()));
//
//    DataSet<Vertex<Long, FlinkVertex>> vertices = flinkTemp
//        .map(new VertexCreator());
//    vertices.print();
  }

  @Test
  public void basicGraphTest() throws Exception {
    Graph<Long, FlinkVertex, NullValue> graph = createSimpleGraph();
    assertEquals(4, graph.getVertices().count());
    assertEquals(3, graph.getEdges().count());
  }

}