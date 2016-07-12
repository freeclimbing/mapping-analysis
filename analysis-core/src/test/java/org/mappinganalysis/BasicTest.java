package org.mappinganalysis;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.GSAConnectedComponents;
import org.apache.flink.types.NullValue;
import org.junit.Test;
import org.mappinganalysis.io.DataLoader;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * basic test class
 * @deprecated
 */
public class BasicTest {

  // todo  test for objectmap not twice lat or lon
//  public void addProperty(String key, Object value) {
//
//    Preconditions.checkArgument(!(key.equals(Utils.LAT) && map.containsKey(Utils.LAT))
//            || !(key.equals(Utils.LON) && map.containsKey(Utils.LON)),
//        map.get(Utils.LAT) + " - " + map.get(Utils.LON) + " LAT or LON already there, new: "
//            + key + ": " + value.toString());

  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  @Test
  public void aggregateTest() throws Exception {
    ObjectMap mapOne = new ObjectMap();
    mapOne.put("distance", 1.0);
    mapOne.put("trigramSim", 0.738549);
    mapOne.put("aggSimValue", 0.8692745);
    ObjectMap mapTwo = new ObjectMap();
    mapTwo.put("distance", 1.0);
    mapTwo.put("trigramSim", 0.957427);
    mapTwo.put("aggSimValue", 0.9787135);
    Edge<Long, ObjectMap> one = new Edge<>(2338L, 3186L, new ObjectMap(mapOne));
    Edge<Long, ObjectMap> two = new Edge<>(1429L, 3186L, new ObjectMap(mapTwo));
    DataSource<Tuple6<Edge<Long, ObjectMap>, Long, String, Integer, Double, Long>> data = env
        .fromElements(
            new Tuple6<>(one, 3186L, "http://data.nytimes.com/", 1, 0.8692745, 1429L),
            new Tuple6<>(one, 3186L, "http://data.nytimes.com/", 1, 0.8692745, 1429L),
            new Tuple6<>(two, 3186L, "http://data.nytimes.com/", 1, 0.9787135, 1429L),
            new Tuple6<>(one, 3186L, "http://data.nytimes.com/", 1, 0.8692745, 1429L),
            new Tuple6<>(one, 3186L, "http://data.nytimes.com/", 1, 0.8692745, 1429L),
            new Tuple6<>(one, 3186L, "http://data.nytimes.com/", 1, 0.8692745, 1429L));

    AggregateOperator<Tuple6<Edge<Long, ObjectMap>, Long, String, Integer, Double, Long>> result = data
        .groupBy(1, 2)
        .sum(3).andMax(4);

    result.print();
  }

  @Test
  public void simpleTest() throws Exception {
    List<Edge<Long, NullValue>> edgeList = Lists.newArrayList();
    edgeList.add(new Edge<>(5680L, 5681L, NullValue.getInstance()));
    edgeList.add(new Edge<>(5680L, 5984L, NullValue.getInstance()));
    Graph<Long, NullValue, NullValue> tmpGraph = Graph.fromCollection(edgeList, env);

    DataSet<Vertex<Long, Long>> vertices = tmpGraph.getVertices()
        .map(new MapFunction<Vertex<Long, NullValue>, Vertex<Long, Long>>() {
          @Override
          public Vertex<Long, Long> map(Vertex<Long, NullValue> value) throws Exception {
            return new Vertex<>(value.getId(), value.getId());
          }
        });

    DataSet<Edge<Long, NullValue>> edges = tmpGraph.getEdges()
        .map(edge -> new Edge<>(edge.getSource(), edge.getTarget(), NullValue.getInstance()))
        .returns(new TypeHint<Edge<Long, NullValue>>() {});

    Graph<Long, Long, NullValue> workingGraph = Graph.fromDataSet(vertices, edges, env);

    DataSet<Tuple2<Long, Long>> verticesWithMinIds = workingGraph
        .run(new GSAConnectedComponents<>(1000))
        .map(new MapFunction<Vertex<Long, Long>, Tuple2<Long, Long>>() {
          @Override
          public Tuple2<Long, Long> map(Vertex<Long, Long> vertex) throws Exception {
            return new Tuple2<>(vertex.getId(), vertex.getValue());

          }
        });

//    final DataSet<Vertex<Long, ObjectMap>> baseVertices = tmpGraph.getVertices()
//        .map(new MapFunction<Vertex<Long, NullValue>, Vertex<Long, ObjectMap>>() {
//          @Override
//          public Vertex<Long, ObjectMap> map(Vertex<Long, NullValue> vertex) throws Exception {
//            ObjectMap prop = new ObjectMap();
//            prop.put(Utils.LABEL, "foo");
//            return new Vertex<>(vertex.getId(), prop);
//          }
//        });

//    Graph<Long, ObjectMap, NullValue> graph = Graph.fromDataSet(baseVertices, tmpGraph.getEdges(), env);

//    final DataSet<Triplet<Long, ObjectMap, ObjectMap>> accumulatedSimValues
//        = SimilarityComputation.computeSimilarities(graph.getTriplets(), "combined");
//
//    // 1. time cc
//    final DataSet<Tuple2<Long, Long>> ccEdges = accumulatedSimValues.project(0, 1);
//    final DataSet<Long> ccVertices = baseVertices.map(new VertexIdMapFunction());
//    final DataSet<Tuple2<Long, Long>> ccResult = FlinkConnectedComponents.compute(ccVertices, ccEdges, 1000);

    verticesWithMinIds.print();

//    DataSet<Vertex<Long, ObjectMap>> ccResultVertices = baseVertices
//        .join(ccResult)
//        .where(0).equalTo(0)
//        .with(new CcResultVerticesJoin()); // deprecated

    // get new edges in components
//    DataSet<Edge<Long, NullValue>> newEdges
//        = GraphUtils.restrictToNewEdges(graph.getEdges(),
//        GraphUtils.computeComponentEdges(ccResultVertices));

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

    DataLoader loader = new DataLoader(env);
    DataSet<Vertex<Long, ObjectMap>> vertices = loader
        .getVertices(Constants.GEO_FULL_NAME)
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