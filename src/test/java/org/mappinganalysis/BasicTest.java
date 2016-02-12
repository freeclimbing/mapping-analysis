package org.mappinganalysis;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.junit.Test;
import org.mappinganalysis.graph.ClusterComputation;
import org.mappinganalysis.graph.FlinkConnectedComponents;
import org.mappinganalysis.io.JDBCDataLoader;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.CcVerticesCreator;
import org.mappinganalysis.model.functions.simcomputation.SimCompUtility;
import org.mappinganalysis.utils.Utils;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * basic test class
 */
public class BasicTest {

  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  @Test
  public void simpleTest() throws Exception {
    List<Edge<Long, NullValue>> edgeList = Lists.newArrayList();
    edgeList.add(new Edge<>(5680L, 5681L, NullValue.getInstance()));
    edgeList.add(new Edge<>(5680L, 5984L, NullValue.getInstance()));
    Graph<Long, NullValue, NullValue> tmpGraph = Graph.fromCollection(edgeList, env);

    final DataSet<Vertex<Long, ObjectMap>> baseVertices = tmpGraph.getVertices()
        .map(new MapFunction<Vertex<Long, NullValue>, Vertex<Long, ObjectMap>>() {
          @Override
          public Vertex<Long, ObjectMap> map(Vertex<Long, NullValue> vertex) throws Exception {
            ObjectMap prop = new ObjectMap();
            prop.put(Utils.LABEL, "foo");
            return new Vertex<>(vertex.getId(), prop);
          }
        });

    Graph<Long, ObjectMap, NullValue> graph = Graph.fromDataSet(baseVertices, tmpGraph.getEdges(), env);

    final DataSet<Triplet<Long, ObjectMap, ObjectMap>> accumulatedSimValues
        = SimCompUtility.computeSimilarities(graph.getTriplets(), "combined");

    // 1. time cc
    final DataSet<Tuple2<Long, Long>> ccEdges = accumulatedSimValues.project(0, 1);
    final DataSet<Long> ccVertices = baseVertices.map(new CcVerticesCreator());
    final DataSet<Tuple2<Long, Long>> ccResult = FlinkConnectedComponents.compute(ccVertices, ccEdges, 1000);

    ccResult.print();

//    DataSet<Vertex<Long, ObjectMap>> ccResultVertices = baseVertices
//        .join(ccResult)
//        .where(0).equalTo(0)
//        .with(new CcResultVerticesJoin()); // deprecated

    // get new edges in components
//    DataSet<Edge<Long, NullValue>> newEdges
//        = ClusterComputation.restrictToNewEdges(graph.getEdges(),
//        ClusterComputation.computeComponentEdges(ccResultVertices));

//    DataSet<Triplet<Long, ObjectMap, ObjectMap>> newSimValues
//        = MappingAnalysisExample.computeSimilarities(
//        Graph.fromDataSet(baseVertices, newEdges, env).getTriplets(), "combined");

//    DataSet<Tuple2<Long, Long>> newSimValuesSimple = newSimValues.project(0, 1);
//    DataSet<Tuple2<Long, Long>> newCcEdges = newSimValuesSimple.union(ccEdges);
//    newCcEdges.print();

    // 2. time cc
//    DataSet<Tuple2<Long, Long>> newCcResult = FlinkConnectedComponents.compute(ccVertices, newCcEdges, 1000);
//    newCcResult.print();
  }

  @SuppressWarnings("unchecked")
  protected Graph<Long, ObjectMap, NullValue> createSimpleGraph() throws Exception {

    JDBCDataLoader loader = new JDBCDataLoader(env);
    DataSet<Vertex<Long, ObjectMap>> vertices = loader
        .getVertices(Utils.GEO_FULL_NAME)
        .filter(new FilterFunction<Vertex<Long, ObjectMap>>() {
          @Override
          public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
            return vertex.getId() == 4795 || vertex.getId() == 5680
                || vertex.getId() == 5984 || vertex.getId() == 5681;
          }
        });

    Edge<Long, NullValue> correctEdge1 = new Edge<>(5680L, 5681L, NullValue.getInstance());
    Edge<Long, NullValue> correctEdge2 = new Edge<>(5680L, 5984L, NullValue.getInstance());
    Edge<Long, NullValue> wrongEdge = new Edge<>(5680L, 4795L, NullValue.getInstance());

    DataSet<Edge<Long, NullValue>> edges
        = env.fromCollection(Sets.newHashSet(correctEdge1, correctEdge2, wrongEdge));
    edges.print();

    return Graph.fromDataSet(vertices, edges, env);

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
    Graph<Long, ObjectMap, NullValue> graph = createSimpleGraph();
    assertEquals(4, graph.getVertices().count());
    assertEquals(3, graph.getEdges().count());
    graph.getVertices().print();
  }

}