package org.mappinganalysis;import com.google.common.collect.Lists;import org.apache.commons.cli.*;import org.apache.flink.api.common.ProgramDescription;import org.apache.flink.api.common.functions.*;import org.apache.flink.api.java.DataSet;import org.apache.flink.api.java.ExecutionEnvironment;import org.apache.flink.api.java.LocalEnvironment;import org.apache.flink.api.java.functions.KeySelector;import org.apache.flink.api.java.tuple.Tuple2;import org.apache.flink.configuration.Configuration;import org.apache.flink.graph.*;import org.apache.flink.types.NullValue;import org.apache.flink.util.Collector;import org.apache.log4j.Logger;import org.mappinganalysis.graph.ClusterComputation;import org.mappinganalysis.graph.FlinkConnectedComponents;import org.mappinganalysis.io.JDBCDataLoader;import org.mappinganalysis.io.functions.EdgeRestrictFlatJoinFunction;import org.mappinganalysis.io.functions.VertexRestrictFlatJoinFunction;import org.mappinganalysis.model.ObjectMap;import org.mappinganalysis.model.Preprocessing;import org.mappinganalysis.model.functions.*;import org.mappinganalysis.model.functions.preprocessing.CcIdAndCompTypeKeySelector;import org.mappinganalysis.model.functions.preprocessing.GenerateHashCcIdGroupReduceFunction;import org.mappinganalysis.model.functions.simcomputation.*;import org.mappinganalysis.model.functions.simsort.*;import org.mappinganalysis.model.functions.typegroupby.TypeGroupBy;import org.mappinganalysis.utils.Stats;import org.mappinganalysis.utils.TypeDictionary;import org.mappinganalysis.utils.Utils;import java.util.*;/** * Mapping analysis example */public class MappingAnalysisExample implements ProgramDescription {  private static final Logger LOG = Logger.getLogger(MappingAnalysisExample.class);  private static ExecutionEnvironment env;// = ExecutionEnvironment.getExecutionEnvironment();  /**   * Command line options   */  private static final String OPTION_LINK_FILTER_PREPROCESSING = "lfp";  private static final String OPTION_PRE_CLUSTER_FILTER = "pcf";  private static final String OPTION_ONLY_INITIAL_CLUSTER = "oic";//  private static final String OPTION_REPRESENTATIVE_STRATEGY = "rs";  private static final String OPTION_DATA_SET_NAME = "ds";  private static final String OPTION_WRITE_STATS = "ws";  private static final String OPTION_CLUSTER_STATS = "cs";  private static final String OPTION_IGNORE_MISSING_PROPERTIES = "imp";  private static final String OPTION_PROCESSING_MODE = "pm";  private static final String OPTION_TYPE_MISS_MATCH_CORRECTION = "tmmc";  private static boolean IS_TYPE_MISS_MATCH_CORRECTION_ACTIVE;  private static boolean STOP_AFTER_INITIAL_CLUSTERING;  private static String PROCESSING_MODE;  private static List<Long> CLUSTER_STATS;  private static Options OPTIONS;  static {    OPTIONS = new Options();    // general    OPTIONS.addOption(OPTION_DATA_SET_NAME, "dataset-name", true,        "Choose one of the datasets [" + Utils.CMD_GEO + " (default), " + Utils.CMD_LL + "].");    OPTIONS.addOption(OPTION_PROCESSING_MODE, "processing-mode", true,        "Choose the processing mode [SimSort + TypeGroupBy (default), simSortOnly].");    OPTIONS.addOption(OPTION_IGNORE_MISSING_PROPERTIES, "ignore-missing-properties", false,        "Do not penalize missing properties on resources in similarity computation process (default: false).");    OPTIONS.addOption(OPTION_ONLY_INITIAL_CLUSTER, "only-initial-cluster", false,        "Don't compute final clusters, stop after preprocessing (default: false).");    // Preprocessing    OPTIONS.addOption(OPTION_LINK_FILTER_PREPROCESSING, "link-filter-preprocessing", false,        "Exclude edges where vertex has several target vertices having equal dataset ontology (default: false).");    OPTIONS.addOption(OPTION_TYPE_MISS_MATCH_CORRECTION, "type-miss-match-correction", false,        "Exclude edges where directly connected source and target vertices have different type property values. " +            "(default: false).");    // to be changed    OPTIONS.addOption(OPTION_PRE_CLUSTER_FILTER, "pre-cluster-filter", true,        "Specify preprocessing filter strategy for entity properties ["            + Utils.DEFAULT_VALUE + " (combined), geo, label, type]");//    OPTIONS.addOption(OPTION_REPRESENTATIVE_STRATEGY, "representative-strategy",//        true, "Set strategy to determine cluster representative (currently only best datasource (default))");    // stats    OPTIONS.addOption(OPTION_WRITE_STATS, "write-stats", false,        "Write statistics to output (default: false).");    Option clusterStats = new Option(OPTION_CLUSTER_STATS, "cluster-stats", true,        "Be more verbose while processing specified cluster ids.");    clusterStats.setArgs(Option.UNLIMITED_VALUES);    OPTIONS.addOption(clusterStats);  }  /**   * main program   * @param args cmd args   * @throws Exception   */  public static void main(String[] args) throws Exception {    Configuration conf = new Configuration();    conf.setLong("taskmanager.network.numberOfBuffers", 65536L);    env =  new LocalEnvironment(conf);    CommandLine cmd = parseArguments(args);    if (cmd == null) {      return;    }    String dataset;    final String optionDataset = cmd.getOptionValue(OPTION_DATA_SET_NAME);    if (optionDataset != null && optionDataset.equals(Utils.CMD_LL)) {      dataset = Utils.LL_FULL_NAME;    } else {      dataset = Utils.GEO_FULL_NAME;    }    Utils.IS_LINK_FILTER_ACTIVE = cmd.hasOption(OPTION_LINK_FILTER_PREPROCESSING);    IS_TYPE_MISS_MATCH_CORRECTION_ACTIVE = cmd.hasOption(OPTION_TYPE_MISS_MATCH_CORRECTION);    PROCESSING_MODE = cmd.getOptionValue(OPTION_PROCESSING_MODE, Utils.DEFAULT_VALUE);    Utils.IGNORE_MISSING_PROPERTIES = cmd.hasOption(OPTION_IGNORE_MISSING_PROPERTIES);    Utils.PRE_CLUSTER_STRATEGY = cmd.getOptionValue(OPTION_PRE_CLUSTER_FILTER, Utils.DEFAULT_VALUE);    STOP_AFTER_INITIAL_CLUSTERING = cmd.hasOption(OPTION_ONLY_INITIAL_CLUSTER);    Utils.PRINT_STATS = cmd.hasOption(OPTION_WRITE_STATS);    String[] clusterStats = cmd.getOptionValues(OPTION_CLUSTER_STATS);    if (clusterStats != null) {      CLUSTER_STATS = Utils.convertWsSparatedString(clusterStats);    }    String ds = dataset.equals(Utils.LL_FULL_NAME) ? "linklion" : "geo";    LOG.info("");    LOG.info("[0] GET DATASET " + ds);    LOG.info("###############");    final Graph<Long, ObjectMap, NullValue> graph = getInputGraph(dataset);    execute(graph);  }  /**   * Mapping analysis computation   * @param preprocGraph input graph   * @throws Exception   */  private static void execute(Graph<Long, ObjectMap, NullValue> preprocGraph) throws Exception {    LOG.info("");    LOG.info("[1] PREPROCESSING");    LOG.info("#################");    preprocGraph = Preprocessing.applyTypeToInternalTypeMapping(preprocGraph, env);    preprocGraph = Preprocessing.applyLinkFilterStrategy(preprocGraph, env, Utils.IS_LINK_FILTER_ACTIVE);    preprocGraph = Preprocessing.applyTypeMissMatchCorrection(preprocGraph, IS_TYPE_MISS_MATCH_CORRECTION_ACTIVE);    preprocGraph = addCcIdsToGraph(preprocGraph);    DataSet<Vertex<Long, ObjectMap>> vertices = preprocGraph.getVertices()        .map(new AddShadingTypeMapFunction())        .groupBy(new CcIdAndCompTypeKeySelector())        .reduceGroup(new GenerateHashCcIdGroupReduceFunction());    KeySelector<Vertex<Long, ObjectMap>, Long> simSortKeySelector = new CcIdKeySelector();    LOG.info("");    LOG.info("[2] INITIAL MATCHING");    // apply similarity functions, similarities are added as edge value and merged (if more than one similarity)    LOG.info("####################");    DataSet<Edge<Long, ObjectMap>> edges = SimCompUtility.computeEdgeSimWithVertices(preprocGraph);    Graph<Long, ObjectMap, ObjectMap> graph = Graph.fromDataSet(vertices, edges, env);    LOG.info("");    LOG.info("[3] INITIAL CLUSTERING");    LOG.info("######################");    /*     * TypeGroupBy: internally compType is used, afterwards typeIntern is used again.     */    if (PROCESSING_MODE.equals(Utils.DEFAULT_VALUE)) {      graph = new TypeGroupBy().execute(graph, 1000);      simSortKeySelector = new HashCcIdKeySelector();    }    /*     * SimSort     */    graph = SimSort.prepare(graph, simSortKeySelector, env);    double minClusterSim = 0.75D;    graph = SimSort.execute(graph, 1000, minClusterSim);    // TODO vertices with vertex status false needs to get a new unique hash ccid    if (STOP_AFTER_INITIAL_CLUSTERING) {      if (Utils.PRINT_STATS) {        LOG.info("### Statistics: ");        DataSet<Tuple2<Long, Long>> result = graph.getVertices()            .map(new MapFunction<Vertex<Long, ObjectMap>, Tuple2<Long, Long>>() {              @Override              public Tuple2<Long, Long> map(Vertex<Long, ObjectMap> vertex) throws Exception {                return new Tuple2<>((long) vertex.getValue().get(Utils.HASH_CC), 1L);              }            }).groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {              @Override              public void reduce(Iterable<Tuple2<Long, Long>> iterable, Collector<Tuple2<Long, Long>> collector) throws Exception {                long result = 0;                long id = 0;                for (Tuple2<Long, Long> tuple1 : iterable) {                  result += tuple1.f1;                  id = tuple1.f0;                }                collector.collect(new Tuple2<>(id, result));              }            });        LOG.info("tuple count: " + result.count());        DataSet<Tuple2<Long, Long>> bar = graph.getVertices()            .map(new MapFunction<Vertex<Long, ObjectMap>, Tuple2<Long, Long>>() {              @Override              public Tuple2<Long, Long> map(Vertex<Long, ObjectMap> vertex) throws Exception {                return new Tuple2<>((long) vertex.getValue().get(Utils.HASH_CC), 1L);              }            }).groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {              @Override              public void reduce(Iterable<Tuple2<Long, Long>> iterable, Collector<Tuple2<Long, Long>> collector) throws Exception {                long result = 0;                long id = 0;                for (Tuple2<Long, Long> tuple1 : iterable) {                  result += tuple1.f1;                  id = tuple1.f0;                }                collector.collect(new Tuple2<>(id, result));              }            });        LOG.info("tuple count: " + bar.count());        Stats.printAccumulatorValues(env, graph.getEdgeIds());        //TODO//        Stats.countPrintGeoPointsPerOntology(preprocGraph);//        Stats.countPrintResourcesPerCc(components1);//        printEdgesSimValueBelowThreshold(allEdgesGraph, accumulatedSimValues);      }    }    /**     * 4. Determination of cluster representative     * - currently: entity from best "data source" (GeoNames > DBpedia > others)     *///    final DataSet<Vertex<Long, ObjectMap>> mergedCluster = graph.getVertices()//        .groupBy(new CcIdKeySelector())//        .reduceGroup(new BestDataSourceAllLabelsGroupReduceFunction());////    if (PRINT_STATS) {////      Stats.countPrintResourcesPerCc(components);//      Stats.printLabelsForMergedClusters(mergedCluster);//    }    /**     * 5. Cluster Refinement     *///    mergedCluster.filter(new FilterFunction<Vertex<Long, ObjectMap>>() {//      @Override//      public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {//         return !(vertex.getValue().get(Utils.CL_VERTICES) instanceof Set);//      }//    });////    DataSet<Edge<Long, Double>> edgesCrossedClusters = mergedCluster//        .cross(mergedCluster)//        .with(new ClusterEdgeCreationCrossFunction())//        .filter(new FilterFunction<Edge<Long, Double>>() {//          @Override//          public boolean filter(Edge<Long, Double> edge) throws Exception {//            return edge.getValue() > 0.7;//          }//        });//    if (PRINT_STATS) {//      edgesCrossedClusters.print();////      System.out.println(edgesCrossedClusters.count());//    }  }  private static Graph<Long, ObjectMap, NullValue> addCcIdsToGraph(      Graph<Long, ObjectMap, NullValue> graph) throws Exception {    final DataSet<Long> ccInputVertices = graph.getVertices()        .map(new CcVerticesCreator());    final DataSet<Tuple2<Long, Long>> components = FlinkConnectedComponents        .compute(ccInputVertices, graph.getEdgeIds(), 1000);    return graph.joinWithVertices(components, new CcIdVertexJoinFunction());  }  /**   * Create the input graph for further analysis,   * restrict to edges where source and target are in vertices set.   * @return graph with vertices and edges.   * @throws Exception   * @param fullDbString complete server+port+db string   */  public static Graph<Long, ObjectMap, NullValue> getInputGraph(String fullDbString)      throws Exception {    JDBCDataLoader loader = new JDBCDataLoader(env);    DataSet<Vertex<Long, ObjectMap>> vertices = loader.getVertices(fullDbString);    // restrict edges to these where source and target are vertices    DataSet<Edge<Long, NullValue>> edges = loader.getEdges(fullDbString)        .leftOuterJoin(vertices)        .where(0).equalTo(0)        .with(new EdgeRestrictFlatJoinFunction())        .leftOuterJoin(vertices)        .where(1).equalTo(0)        .with(new EdgeRestrictFlatJoinFunction());    // delete vertices without any edges due to restriction    DataSet<Vertex<Long, ObjectMap>> left = vertices        .leftOuterJoin(edges)        .where(0).equalTo(0)        .with(new VertexRestrictFlatJoinFunction()).distinct(0);    DataSet<Vertex<Long, ObjectMap>> finalVertices = vertices        .leftOuterJoin(edges)        .where(0).equalTo(1)        .with(new VertexRestrictFlatJoinFunction()).distinct(0)        .union(left);    return Graph.fromDataSet(finalVertices, edges, env);  }  /**   * Parses the program arguments or returns help if args are empty.   *   * @param args program arguments   * @return command line which can be used in the program   */  private static CommandLine parseArguments(String[] args) throws ParseException {    if (args.length == 0) {      HelpFormatter formatter = new HelpFormatter();      formatter.printHelp(MappingAnalysisExample.class.getName(), OPTIONS, true);      return null;    }    CommandLineParser parser = new BasicParser();    return parser.parse(OPTIONS, args);  }  @Override  public String getDescription() {    return MappingAnalysisExample.class.getName();  }  public static class AddAggSimVertexJoinFunction implements JoinFunction<Vertex<Long, ObjectMap>, Tuple2<Long, Double>, Vertex<Long, ObjectMap>> {    @Override    public Vertex<Long, ObjectMap> join(Vertex<Long, ObjectMap> vertex, Tuple2<Long, Double> tuple) throws Exception {      vertex.getValue().put(Utils.VERTEX_AGG_SIM_VALUE, tuple.f1);      return vertex;    }  }  public static class LowSimFilterFunction implements FilterFunction<Vertex<Long, ObjectMap>> {    private final double threshold;    public LowSimFilterFunction(double t) {      this.threshold = t;    }    @Override    public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {      return (double) vertex.getValue().get(Utils.VERTEX_AGG_SIM_VALUE) < threshold;    }  }  private static class AddShadingTypeMapFunction implements MapFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {    @Override    public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {      String vertexType = vertex.getValue().get(Utils.TYPE_INTERN).toString();      if (TypeDictionary.TYPE_SHADINGS.containsKey(vertexType)          || TypeDictionary.TYPE_SHADINGS.containsValue(vertexType)) {        switch (vertexType) {          case "School":            vertexType = "ArchitecturalStructure";            break;          case "Mountain":            vertexType = "Island";            break;          case "Settlement":          case "Country":            vertexType = "AdministrativeRegion";            break;        }      }      vertex.getValue().put(Utils.COMP_TYPE, vertexType);      return vertex;    }  }}