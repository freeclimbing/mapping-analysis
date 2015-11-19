package org.mappinganalysis;import com.google.common.collect.Maps;import com.google.common.primitives.Doubles;import org.apache.flink.api.common.functions.*;import org.apache.flink.api.java.DataSet;import org.apache.flink.api.java.ExecutionEnvironment;import org.apache.flink.api.java.aggregation.Aggregations;import org.apache.flink.api.java.functions.KeySelector;import org.apache.flink.api.java.tuple.Tuple2;import org.apache.flink.api.java.tuple.Tuple4;import org.apache.flink.graph.*;import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;import org.apache.flink.types.NullValue;import org.apache.flink.util.Collector;import org.apache.log4j.Logger;import org.mappinganalysis.graph.FlinkConnectedComponents;import org.mappinganalysis.io.JDBCDataLoader;import org.mappinganalysis.model.FlinkVertex;import org.mappinganalysis.model.PropertyHelper;import org.mappinganalysis.model.functions.*;import org.mappinganalysis.utils.TypeDictionary;import java.util.List;import java.util.Map;/** * Read data from MySQL database via JDBC into Apache Flink. */public class MySQLToFlink {  private static final Logger LOG = Logger.getLogger(MySQLToFlink.class);  private static final String CC_ID = "ccId";  private static final String CL_REPRESENTATIVE = "clusterRepresentative";  private static final String TYPE_KEY = "type";  private static final String INTERN_TYPE_KEY = "typeIntern";  private static final String ONTOLOGY = "ontology";  public static void main(String[] args) throws Exception {    // check if each edge points to existing vertices    // System.out.println(graph.validate(new InvalidVertexIdsValidator<Integer, String, NullValue>()));    Graph<Long, FlinkVertex, NullValue> graph = getInputGraph();    graph.getVertices().print();    /**     * 1. PREPROCESSING     * - comment line if not needed     */    graph = applyLinkFilterStrategy(graph);    graph.getVertices().print();    graph = applyTypePreprocessing(graph);    /**     * 2. INITIAL MATCHING     * - apply similarity functions, similarities are added as edge value and merged (if more than one similarity)     */    final DataSet<Triplet<Long, FlinkVertex, NullValue>> baseTriplets = graph.getTriplets();    DataSet<Triplet<Long, FlinkVertex, Map<String, Object>>> accumulatedSimValues        = initialSimilarityComputation(baseTriplets);    accumulatedSimValues.print();    /**     * 3. INITIAL CLUSTERING     * - connected components     */    final DataSet<Tuple2<Long, Long>> ccEdges = accumulatedSimValues.project(0, 1);    final DataSet<Long> ccVertices = graph.getVertices().map(new CcVerticesCreator());    FlinkConnectedComponents connectedComponents = new FlinkConnectedComponents();    final DataSet<Tuple2<Long, Long>> ccResult = connectedComponents        .compute(ccVertices, ccEdges, 1000);    // update all vertices with ccId    DataSet<Vertex<Long, FlinkVertex>> vertices = graph.getVertices()        .join(ccResult)        .where(0)        .equalTo(0)        .with(new CcResultVerticesJoin());    // update triplets with edge props    DataSet<Edge<Long, Map<String, Object>>> joinedEdges = graph        .getEdges()        .join(accumulatedSimValues)        .where(0, 1)        .equalTo(0, 1)        .with(new CcResultEdgesJoin());    DataSet<Vertex<Long, FlinkVertex>> mergedCluster = vertices        .groupBy(new KeySelector<Vertex<Long, FlinkVertex>, Long>() {          @Override          public Long getKey(Vertex<Long, FlinkVertex> vertex) throws Exception {            return (long) vertex.getValue().getProperties().get(CC_ID);          }        })        .reduceGroup(new GroupReduceFunction<Vertex<Long, FlinkVertex>, Vertex<Long, FlinkVertex>>() {          @Override          public void reduce(Iterable<Vertex<Long, FlinkVertex>> iterable,                             Collector<Vertex<Long, FlinkVertex>> collector) throws Exception {            FlinkVertex result = new FlinkVertex();            Map<String, Object> resultProps = Maps.newHashMap();            boolean isRepresentative = false;            for (Vertex<Long, FlinkVertex> vertex : iterable) {              if (!isRepresentative) {                result.setId(vertex.getId());                isRepresentative = true;              }              Map<String, Object> properties = vertex.getValue().getProperties();              // TODO duplicate property values are not filtered              for (String key : properties.keySet()) {                resultProps = PropertyHelper.addValueToProperties(resultProps, properties.get(key), key);              }            }            result.setProperties(resultProps);//            System.out.println("result vertex: " + result);            collector.collect(new Vertex<>(result.getId(), result));          }        });    mergedCluster.print();//    System.out.println(mergedCluster.count());// first try community detection... needed?//    DataSet<Edge<Long, Double>> clusterEdges = joinedEdges//        .map(new MapFunction<Edge<Long, Map<String, Object>>, Edge<Long, Double>>() {//          @Override//          public Edge<Long, Double> map(Edge<Long, Map<String, Object>> edge) throws Exception {//            // TODO -1 rly good here?//            return new Edge<>(edge.getSource(),//                edge.getTarget(),//                edge.getValue().containsKey("distance") ? (double) edge.getValue().get("distance") : -1);//          }//        });//    DataSet<Vertex<Long, Long>> clusterVertices = vertices//        .map(new MapFunction<Vertex<Long, FlinkVertex>, Vertex<Long, Long>>() {//          @Override//          public Vertex<Long, Long> map(Vertex<Long, FlinkVertex> vertex) throws Exception {//            // fix 2. id//            return new Vertex<>(vertex.getId(), vertex.getId());//          }//        });//    Graph<Long, Long, Double> resultGraph = Graph//        .fromDataSet(clusterVertices, clusterEdges, ExecutionEnvironment.createLocalEnvironment());////// run Label Propagation for 30 iterations to detect communities on the input graph////    DataSet<Vertex<Long, Long>> communityVertices =//    Graph<Long, Long, Double> commGraph = resultGraph.run(new CommunityDetection<Long>(30, 0.5));        countPrintResourcesPerCc(ccResult);//        countPrintGeoPointsPerOntology();  }  private static DataSet<Triplet<Long, FlinkVertex, Map<String, Object>>>  initialSimilarityComputation(DataSet<Triplet<Long, FlinkVertex, NullValue>> baseTriplets) {    DataSet<Triplet<Long, FlinkVertex, Map<String, Object>>> geoSimilarity        = baseTriplets        .filter(new EmptyGeoCodeFilter())        .map(new GeoCodeSimFunction())        .filter(new GeoCodeThreshold());    DataSet<Triplet<Long, FlinkVertex, Map<String, Object>>> exactSim        = baseTriplets        .map(new SimilarTripletExtractor())        .filter(new TripletFilter());    DataSet<Triplet<Long, FlinkVertex, Map<String, Object>>> typeSim        = baseTriplets        .map(new TypeSimilarityMapper())        .filter(new TypeFilter());    DataSet<Triplet<Long, FlinkVertex, Map<String, Object>>> accumulatedSimValues = geoSimilarity        .fullOuterJoin(exactSim)        .where(0, 1)        .equalTo(0, 1)        .with(new JoinSimilarityValueFunction());    accumulatedSimValues = accumulatedSimValues        .fullOuterJoin(typeSim)        .where(0, 1)        .equalTo(0, 1)        .with(new JoinSimilarityValueFunction());    return accumulatedSimValues;  }  // duplicate methods in emptygeocodefilter  private static void countPrintGeoPointsPerOntology() throws Exception {    Graph<Long, FlinkVertex, NullValue> tgraph = getInputGraph();    tgraph.getVertices()        .filter(new FilterFunction<Vertex<Long, FlinkVertex>>() {          @Override          public boolean filter(Vertex<Long, FlinkVertex> vertex) throws Exception {            Map<String, Object> props = vertex.getValue().getProperties();            if (props.containsKey("lat") && props.containsKey("lon")) {              Object lat = props.get("lat");              Object lon = props.get("lon");              return ((getDouble(lat) == null) || (getDouble(lon) == null)) ? Boolean.FALSE : Boolean.TRUE;            } else {              return Boolean.FALSE;            }          }          private Double getDouble(Object latlon) {            if (latlon instanceof List) {              return Doubles.tryParse(((List) latlon).get(0).toString());            } else {              return Doubles.tryParse(latlon.toString());            }          }        })        .groupBy(new KeySelector<Vertex<Long, FlinkVertex>, String>() {          @Override          public String getKey(Vertex<Long, FlinkVertex> vertex) throws Exception {            return (String) vertex.getValue().getProperties().get(ONTOLOGY);          }        })        .reduceGroup(new GroupReduceFunction<Vertex<Long, FlinkVertex>, Vertex<Long, FlinkVertex>>() {          @Override          public void reduce(Iterable<Vertex<Long, FlinkVertex>> iterable, Collector<Vertex<Long, FlinkVertex>> collector) throws Exception {            long count = 0;            FlinkVertex result = new FlinkVertex();            Map<String, Object> resultProps = Maps.newHashMap();            boolean isVertexPrepared = false;            for (Vertex<Long, FlinkVertex> vertex : iterable) {              count++;              if (!isVertexPrepared) {                resultProps = vertex.getValue().getProperties();                result.setId(vertex.getId());                isVertexPrepared = true;              }            }            resultProps.put("count", count);            result.setProperties(resultProps);            collector.collect(new Vertex<>(result.getId(), result));          }        })        .print();  }  private static Graph<Long, FlinkVertex, NullValue> applyTypePreprocessing(Graph<Long, FlinkVertex, NullValue> graph) {    DataSet<Vertex<Long, FlinkVertex>> vertices = graph.getVertices()        .map(new MapFunction<Vertex<Long, FlinkVertex>, Vertex<Long, FlinkVertex>>() {          @Override          public Vertex<Long, FlinkVertex> map(Vertex<Long, FlinkVertex> vertex) throws Exception {            System.out.println("##############################");            System.out.println(vertex.getId());            System.out.println("##############################");            System.out.println("class: " + vertex.getValue().getClass());            FlinkVertex flinkVertex = vertex.getValue();            Map<String, Object> properties = flinkVertex.getProperties();            if (properties.containsKey(TYPE_KEY)) {              // get relevant key and translate with custom dictionary for internal use              Object oldValue = properties.get(TYPE_KEY);              String resultType;              if (oldValue instanceof List) {                List<String> values = Lists.newArrayList((List<String>) oldValue);                resultType = getDictValue(values);              } else {                resultType = getDictValue(Lists.newArrayList((String) oldValue));              }              properties.put(INTERN_TYPE_KEY, resultType);//              System.out.println(vertex);              return vertex;            }            return vertex;          }        });    return Graph.fromDataSet(vertices,        graph.getEdges(),        ExecutionEnvironment.createLocalEnvironment());  }  private static String getDictValue(List<String> values) {    for (String value : values) {//      if (TypeDictionary.GN_TYPE.contains(value)) {//        value =//        //getGnType(value);//      }      if (TypeDictionary.PRIMARY_TYPE.containsKey(value)) {        return TypeDictionary.PRIMARY_TYPE.get(value);      }    }    for (String value : values) {      if (TypeDictionary.SECONDARY_TYPE.containsKey(value)) {        return TypeDictionary.SECONDARY_TYPE.get(value);      }    }    for (String value : values) {      if (TypeDictionary.TERTIARY_TYPE.containsKey(value)) {        return TypeDictionary.TERTIARY_TYPE.get(value);      }    }    return "-1";  }  /**   * Count resources per component for a given flink connected component result set.   * @param ccResult dataset to be analyzed   * @throws Exception   */  private static void countPrintResourcesPerCc(DataSet<Tuple2<Long, Long>> ccResult) throws Exception {    System.out.println(ccResult.project(1).distinct().count());    List<Tuple2<Long, Long>> ccGeoList = ccResult        .groupBy(1)        .reduceGroup(new GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {          @Override          public void reduce(Iterable<Tuple2<Long, Long>> component, Collector<Tuple2<Long, Long>> out) throws Exception {            long count = 0;            long id = 0;            for (Tuple2<Long, Long> vertex : component) {              if (vertex.f1 == 4794 || vertex.f1 == 5680) {                System.out.println(vertex);              }              count++;              id = vertex.f1;            }            out.collect(new Tuple2<>(id, count));          }        })        .collect();    int one = 0;    int two = 0;    int three = 0;    int four = 0;    int five = 0;    int six = 0;    int seven = 0;    for (Tuple2<Long, Long> tuple2 : ccGeoList) {      if (tuple2.f1 == 1) {        one++;      } else if (tuple2.f1 == 2) {        two++;      } else if (tuple2.f1 == 3) {        three++;      } else if (tuple2.f1 == 4) {        four++;      } else if (tuple2.f1 == 5) {        five++;      } else if (tuple2.f1 == 6) {        six++;      } else if (tuple2.f1 == 7) {        seven++;      }    }    System.out.println("one: " + one + " two: " + two + " three: " + three +        " four: " + four + " five: " + five + " six: " + six + " seven: " + seven);  }  /**   * Create the input graph for further analysis.   * @return graph with vertices and edges.   * @throws Exception   */  private static Graph<Long, FlinkVertex, NullValue> getInputGraph() throws Exception {    ExecutionEnvironment environment = ExecutionEnvironment.createLocalEnvironment();    JDBCDataLoader loader = new JDBCDataLoader(environment);    DataSet<Edge<Long, NullValue>> edges = loader.getEdges();    DataSet<Vertex<Long, FlinkVertex>> vertices = loader.getVertices()        .map(new VertexCreator());    return Graph.fromDataSet(vertices, edges, environment);  }  /**   * Preprocessing strategy to restrict resources to have only one counterpart in every target ontology.   *   * First strategy: delete all links which are involved in 1:n mappings   * @param graph input graph   * @return output graph   * @throws Exception   */  private static Graph<Long, FlinkVertex, NullValue> applyLinkFilterStrategy(Graph<Long, FlinkVertex, NullValue> graph)      throws Exception {    DataSet<Edge<Long, NullValue>> edgesNoDuplicates = graph        .groupReduceOnNeighbors(new NeighborOntologyFunction(), EdgeDirection.OUT)        .groupBy(1, 2)        .aggregate(Aggregations.SUM, 3)        .filter(new ExcludeOneToManyOntologiesFilter())        .map(new MapFunction<Tuple4<Edge<Long, NullValue>, Long, String, Integer>,            Edge<Long, NullValue>>() {          @Override          public Edge<Long, NullValue> map(Tuple4<Edge<Long, NullValue>, Long, String, Integer> tuple)              throws Exception {            return tuple.f0;          }        });    return Graph.fromDataSet(graph.getVertices(),        edgesNoDuplicates,        ExecutionEnvironment.createLocalEnvironment());  }  public static class CcResultVerticesJoin implements JoinFunction<Vertex<Long, FlinkVertex>,  Tuple2<Long, Long>, Vertex<Long, FlinkVertex>> {    @Override    public Vertex<Long, FlinkVertex> join(Vertex<Long, FlinkVertex> vertex, Tuple2<Long, Long> tuple)    throws Exception {      vertex.getValue().getProperties().put(CC_ID, tuple.f1);      return vertex;    }  }  public static class CcResultEdgesJoin implements JoinFunction<Edge<Long,NullValue>,      Triplet<Long,FlinkVertex,Map<String,Object>>, Edge<Long, Map<String, Object>>> {    @Override    public Edge<Long, Map<String, Object>> join(Edge<Long, NullValue> edge,        Triplet<Long, FlinkVertex, Map<String, Object>> triplet)    throws Exception {      return new Edge<>(edge.getSource(), edge.getTarget(), triplet.f4);    }  }  public static class FilterExactMatch implements FilterFunction<Triplet<Long, FlinkVertex, Map<String, Object>>> {    @Override    public boolean filter(Triplet<Long, FlinkVertex, Map<String, Object>> triplet) throws Exception {      return triplet.getEdge().getValue().containsKey("exactMatch");    }  }  public static class FilterSizeMinTwo implements FilterFunction<Triplet<Long, FlinkVertex, Map<String, Object>>> {    @Override    public boolean filter(Triplet<Long, FlinkVertex, Map<String, Object>> triplet) throws Exception {      return triplet.getEdge().getValue().size() > 1;    }  }  private static class JoinFilterStrategyFunction      implements JoinFunction<Edge<Long, NullValue>, Edge<Long, NullValue>, Edge<Long, NullValue>> {    @Override    public Edge<Long, NullValue> join(Edge<Long, NullValue> edge, Edge<Long, NullValue> deleteEdge) throws Exception {      return edge;    }  }  private static class ExcludeOneToManyOntologiesFilter      implements FilterFunction<Tuple4<Edge<Long, NullValue>, Long, String, Integer>> {    @Override    public boolean filter(Tuple4<Edge<Long, NullValue>, Long, String, Integer> tuple) throws Exception {      return tuple.f3 < 2;    }  }  private static class LabelExtractor implements MapFunction<FlinkVertex, Vertex<Long, String>> {    @Override    public Vertex<Long, String> map(FlinkVertex flinkVertex) throws Exception {      Object label = flinkVertex.getValue().get("label");      return new Vertex<>(flinkVertex.getId(), (label != null) ? label.toString() : "null");    }  }  private static class TypeFilter implements FilterFunction<Triplet<Long, FlinkVertex, Map<String, Object>>> {    @Override    public boolean filter(Triplet<Long, FlinkVertex, Map<String, Object>> weightedTriplet) throws Exception {      Map<String, Object> props = weightedTriplet.getEdge().getValue();      return props.containsKey("typeMatch") && (float) props.get("typeMatch") == 1f;    }  }  private static class GeoCodeThreshold      implements FilterFunction<Triplet<Long, FlinkVertex, Map<String, Object>>> {    @Override    public boolean filter(Triplet<Long, FlinkVertex, Map<String, Object>> distanceThreshold) throws Exception {      return ((double) distanceThreshold.getEdge().getValue().get("distance")) < 50000;    }  }  private static class CcVerticesCreator implements MapFunction<Vertex<Long, FlinkVertex>, Long> {    @Override    public Long map(Vertex<Long, FlinkVertex> flinkVertex) throws Exception {      return flinkVertex.getId();    }  }}