package org.mappinganalysis;import com.google.common.collect.Lists;import com.google.common.primitives.Floats;import org.apache.commons.cli.*;import org.apache.flink.api.common.ProgramDescription;import org.apache.flink.api.common.functions.*;import org.apache.flink.api.java.DataSet;import org.apache.flink.api.java.ExecutionEnvironment;import org.apache.flink.api.java.LocalEnvironment;import org.apache.flink.api.java.tuple.Tuple2;import org.apache.flink.configuration.Configuration;import org.apache.flink.graph.*;import org.apache.flink.graph.spargel.VertexCentricConfiguration;import org.apache.flink.types.NullValue;import org.apache.log4j.Logger;import org.mappinganalysis.graph.ClusterComputation;import org.mappinganalysis.graph.FlinkConnectedComponents;import org.mappinganalysis.io.JDBCDataLoader;import org.mappinganalysis.io.functions.EdgeRestrictFlatJoinFunction;import org.mappinganalysis.io.functions.VertexRestrictFlatJoinFunction;import org.mappinganalysis.model.ObjectMap;import org.mappinganalysis.model.Preprocessing;import org.mappinganalysis.model.functions.*;import org.mappinganalysis.model.functions.clustering.*;import org.mappinganalysis.model.functions.preprocessing.CcIdAndTypeKeySelector;import org.mappinganalysis.model.functions.preprocessing.GenerateNewCcIdGroupReduceFunction;import org.mappinganalysis.model.functions.simcomputation.EmptyGeoCodeFilter;import org.mappinganalysis.model.functions.simcomputation.GeoCodeSimMapper;import org.mappinganalysis.model.functions.simcomputation.TrigramSimilarityMapper;import org.mappinganalysis.model.functions.simcomputation.TypeSimilarityMapper;import org.mappinganalysis.model.functions.simsort.AggSimValueEdgeMapFunction;import org.mappinganalysis.model.functions.simsort.SimSortMessagingFunction;import org.mappinganalysis.model.functions.simsort.SimSortVertexUpdateFunction;import org.mappinganalysis.model.functions.simsort.TripletToEdgeMapFunction;import org.mappinganalysis.model.functions.stats.ResultComponentSelectionFilter;import org.mappinganalysis.model.functions.stats.ResultEdgesSelectionFilter;import org.mappinganalysis.model.functions.stats.ResultVerticesSelectionFilter;import org.mappinganalysis.model.functions.typegroupby.NoTypeVertexUpdateFunction;import org.mappinganalysis.model.functions.typegroupby.VertexAndSimValueMessagingFunction;import org.mappinganalysis.utils.Utils;import java.util.*;/** * Mapping analysis example */public class MappingAnalysisExample implements ProgramDescription {  private static final Logger LOG = Logger.getLogger(MappingAnalysisExample.class);  private static ExecutionEnvironment env;// = ExecutionEnvironment.getExecutionEnvironment();  /**   * Command line options   */  private static final String OPTION_DELETE_LINKS_PREPROCESSING = "dlp";  private static final String OPTION_PRE_CLUSTER_FILTER = "pcf";  private static final String OPTION_ONLY_INITIAL_CLUSTER = "oic";//  private static final String OPTION_REPRESENTATIVE_STRATEGY = "rs";  private static final String OPTION_DATA_SET_NAME = "ds";  private static final String OPTION_WRITE_STATS = "ws";  private static final String OPTION_CLUSTER_STATS = "cs";  private static boolean IS_LINK_FILTER_ACTIVE;  private static String PRE_CLUSTER_STRATEGY;  private static boolean STOP_AFTER_INITIAL_CLUSTERING;  private static boolean PRINT_STATS;  private static List<Long> CLUSTER_STATS;  private static Options OPTIONS;  static {    OPTIONS = new Options();    OPTIONS.addOption(OPTION_DATA_SET_NAME, "dataset-name", true,        "Choose one of the datasets [" + Utils.CMD_GEO + " (default), " + Utils.CMD_LL + "].");    OPTIONS.addOption(OPTION_DELETE_LINKS_PREPROCESSING, "delete-links-preprocessing", false,        "Use delete 1:n strategy for initial link check (default: false).");    OPTIONS.addOption(OPTION_PRE_CLUSTER_FILTER, "pre-cluster-filter", true,        "Specify preprocessing filter strategy for entity properties ["            + Utils.CMD_COMBINED + " (default), geo, label, type]");    OPTIONS.addOption(OPTION_ONLY_INITIAL_CLUSTER, "only-initial-cluster", false,        "Don't compute final clusters, stop after preprocessing (default: false).");//    OPTIONS.addOption(OPTION_REPRESENTATIVE_STRATEGY, "representative-strategy",//        true, "Set strategy to determine cluster representative (currently only best datasource (default))");    OPTIONS.addOption(OPTION_WRITE_STATS, "write-stats", false,        "Write statistics to output (default: false).");    Option clusterStats = new Option(OPTION_CLUSTER_STATS, "cluster-stats", true,        "Be more verbose while processing specified cluster ids.");    clusterStats.setArgs(Option.UNLIMITED_VALUES);    OPTIONS.addOption(clusterStats);  }  /**   * main program   * @param args cmd args   * @throws Exception   */  public static void main(String[] args) throws Exception {    Configuration conf = new Configuration();    conf.setLong("taskmanager.network.numberOfBuffers", 65536L);    env =  new LocalEnvironment(conf);    CommandLine cmd = parseArguments(args);    if (cmd == null) {      return;    }    String dataset;    final String optionDataset = cmd.getOptionValue(OPTION_DATA_SET_NAME);    if (optionDataset != null && optionDataset.equals(Utils.CMD_LL)) {      dataset = Utils.LL_FULL_NAME;    } else {      dataset = Utils.GEO_FULL_NAME;    }    IS_LINK_FILTER_ACTIVE = cmd.hasOption(OPTION_DELETE_LINKS_PREPROCESSING);    PRE_CLUSTER_STRATEGY = cmd.getOptionValue(OPTION_PRE_CLUSTER_FILTER, Utils.CMD_COMBINED);    STOP_AFTER_INITIAL_CLUSTERING = cmd.hasOption(OPTION_ONLY_INITIAL_CLUSTER);    PRINT_STATS = cmd.hasOption(OPTION_WRITE_STATS);    String[] clusterStats = cmd.getOptionValues(OPTION_CLUSTER_STATS);    if (clusterStats != null) {      CLUSTER_STATS = Utils.convertWsSparatedString(clusterStats);    }    /**     * 0. Get data     */    final Graph<Long, ObjectMap, NullValue> graph = getInputGraph(dataset);    execute(graph);  }  /**   * Mapping analysis computation   * @param preprocGraph input graph   * @throws Exception   */  private static void execute(Graph<Long, ObjectMap, NullValue> preprocGraph) throws Exception {    /**     * [1] PREPROCESSING     */    LOG.info("[1] PREPROCESSING");//    preprocGraph = Preprocessing.applyLinkFilterStrategy(preprocGraph, env, IS_LINK_FILTER_ACTIVE);    preprocGraph = Preprocessing.applyTypeToInternalTypeMapping(preprocGraph, env);//    preprocGraph = Preprocessing.applyTypeMissMatchCorrection(preprocGraph);    /**     * [2] INITIAL MATCHING     * - apply similarity functions, similarities are added as edge value and merged     * (if more than one similarity)     */    LOG.info("[2] INITIAL MATCHING");//    Graph<Long, ObjectMap, NullValue> allEdgesGraph = createAllEdgesGraph(preprocGraph);    /**     * [3] INITIAL CLUSTERING     * - connected components + split     */    LOG.info("[3] INITIAL CLUSTERING");    preprocGraph = addCcIdsToGraph(preprocGraph);    DataSet<Edge<Long, ObjectMap>> edges = addSimilaritiesToEdges(preprocGraph);    edges.filter(new FilterFunction<Edge<Long, ObjectMap>>() {      @Override      public boolean filter(Edge<Long, ObjectMap> edge) throws Exception {        if (  edge.getValue().containsKey(Utils.TYPE_MATCH) &&            Floats.compare((float) edge.getValue().get(Utils.TYPE_MATCH), 0.8f) == 0) {          return true;        } else {          return false;        }      }    }).print();    if (edges != null) {      return;    }    DataSet<Vertex<Long, ObjectMap>> vertices = preprocGraph.getVertices()        .groupBy(new CcIdAndTypeKeySelector())        .reduceGroup(new GenerateNewCcIdGroupReduceFunction());    Graph<Long, ObjectMap, ObjectMap> graph = Graph.fromDataSet(vertices, edges, env);    VertexCentricConfiguration parameters = new VertexCentricConfiguration();    parameters.setName("Type-based Cluster Generation Iteration");    parameters.setDirection(EdgeDirection.ALL);    // assign non-type vertices to best matching    Graph<Long, ObjectMap, ObjectMap> iterationGraph = graph.runVertexCentricIteration(        new NoTypeVertexUpdateFunction(),        new VertexAndSimValueMessagingFunction(), 100, parameters);    // use hash cc to compute all edges TODO fix in clustercomputation    DataSet<Edge<Long, NullValue>> allEdges = iterationGraph.getVertices()        .coGroup(iterationGraph.getVertices())        .where(new HashCcIdKeySelector())        .equalTo(new HashCcIdKeySelector())        .with(new EdgeExtractCoGroupFunction());    allEdges = ClusterComputation.getDistinctSimpleEdges(allEdges);    Graph<Long, ObjectMap, NullValue> aggGraph = Graph        .fromDataSet(iterationGraph.getVertices(), allEdges, env);    DataSet<Edge<Long, ObjectMap>> allSimEdges = addSimilaritiesToEdges(aggGraph);    Graph<Long, ObjectMap, ObjectMap> aggItGraph = Graph        .fromDataSet(aggGraph.getVertices(), allSimEdges, env);    // TODO init vert agg sim values?    aggItGraph = aggItGraph.mapVertices(new MapFunction<Vertex<Long,ObjectMap>, ObjectMap>() {      @Override      public ObjectMap map(Vertex<Long, ObjectMap> vertex) throws Exception {        vertex.getValue().put(Utils.VERTEX_AGG_SIM_VALUE, Utils.DEFAULT_VERTEX_SIM);        return vertex.getValue();      }    });    // TODO single vertex ?    VertexCentricConfiguration aggParameters = new VertexCentricConfiguration();    aggParameters.setName("TypeGroupBy");    aggParameters.setDirection(EdgeDirection.ALL);    aggItGraph = aggItGraph.runVertexCentricIteration(        new SimSortVertexUpdateFunction(),        new SimSortMessagingFunction(), 10, aggParameters);//    // TODO iteration to flag vertices based on low aggregated edge similarity//    // TODO not rly working because of all bad vertices are eliminated in 1. step//    Graph<Long, ObjectMap, ObjectMap> gsaGraph = graph//        .mapVertices(new ActivateAllVerticesMapFunction())//        .runGatherSumApplyIteration(//            new GatherEdgeSimsBasedOnVertexStatusFunction(),//            new SumEdgeSimsAndCountFunction(),//            new ApplyVertexAggSimChangeVertexStatusFunction(0.6), 100);////    DataSet<Vertex<Long, ObjectMap>> lowSimVertices = gsaGraph.getVertices()//        .filter(new FilterFunction<Vertex<Long, ObjectMap>>() {//          @Override//          public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {//            return !(boolean) vertex.getValue().get(Utils.VERTEX_STATUS);//          }//        });//    aggVertexSimGraph = aggVertexSimGraph.removeVertices(lowSimVertices.collect());////    // find max ccId over all vertices (should always be only one,//    // because only one vertex of each (unique) component can be deleted)//    final long maxCcId = components.aggregate(Aggregations.MAX, 1).collect().get(0).f1;////    // take old vertex id as base ccId and add the previously found max ccId to have unique values//    DataSet<Vertex<Long, ObjectMap>> newVertices = lowSimVertices//        .map(new MapFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>>() {//          @Override//          public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {//            ObjectMap value = vertex.getValue();//            value.put(Utils.CC_ID, (long) value.get(Utils.CC_ID) + maxCcId);//            return vertex;//          }//        });////    DataSet<Vertex<Long, ObjectMap>> unionVerts = aggVertexSimGraph.getVertices().union(newVertices);////    Graph<Long, ObjectMap, ObjectMap> unionGraph//        = Graph.fromDataSet(unionVerts, aggVertexSimGraph.getEdges(), env);//    List<Vertex<Long, ObjectMap>> bla = newVertices.collect();//    aggVertexSimGraph = aggVertexSimGraph.addVertices(bla);    if (STOP_AFTER_INITIAL_CLUSTERING) {      if (PRINT_STATS) {        LOG.info("### Statistics: ");//        Stats.countPrintGeoPointsPerOntology(preprocGraph);        LOG.info("### 1. part: ");        aggItGraph = aggItGraph.filterOnVertices(new FilterFunction<Vertex<Long, ObjectMap>>() {          @Override          public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {//            LOG.info(vertex);            if ((double) vertex.getValue().get(Utils.VERTEX_AGG_SIM_VALUE) < 0.6) {              return true;            } else {              return false;            }          }        });        LOG.info(aggItGraph.getVertices().count());//        DataSet<Tuple2<Long, Long>> components1 = iterationGraph.getVertices()//            .map(new ComponentsMapFunction());////        Stats.countPrintResourcesPerCc(components1);////        LOG.info("CC:");        List<Long> ccList = Lists.newArrayList(3711L, 2813L, 1560L, 3703L, 4482L);        writeCcToLog(aggItGraph, ccList);        LOG.info("### end part: ");        writeEdgesToLog(graph, ccList);////        LOG.info("Vertices: ");//        writeVerticesToLog(iterationGraph, CLUSTER_STATS);//        Stats.printAccumulatorValues(env, aggVertexSimGraph.getEdgeIds());//        LOG.info("first: " + iterateInputGraph.getVertexIds().count());//        LOG.info("sec: " + aggVertexSimGraph.getVertices()//            .filter(new FilterFunction<Vertex<Long, ObjectMap>>() {//              @Override//              public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {//                return !(boolean) vertex.getValue().get(Utils.VERTEX_STATUS);//              }//            }).count());////        Stats.countPrintResourcesPerCc(unionVerts.map(new MapFunction<Vertex<Long, ObjectMap>, Tuple2<Long, Long>>() {//          @Override//          public Tuple2<Long, Long> map(Vertex<Long, ObjectMap> vertex) throws Exception {//            return new Tuple2<>(vertex.getId(), (long) vertex.getValue().get(Utils.CC_ID));//          }//        }));//        LOG.info("lowSim: " + lowSimVertices.count());//        LOG.info("low 0.6: " + lowSimVertices.filter(new LowSimFilterFunction(0.6)).count());//        LOG.info("low 0.7: " + lowSimVertices.filter(new LowSimFilterFunction(0.7)).count());//        newVertices.print();        // TODO//        // get low confidence vertices//        DataSet<Vertex<Long, ObjectMap>> aggVertexFilter = aggVertices//            .filter(new FilterFunction<Vertex<Long, ObjectMap>>() {//              @Override//              public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {//                ObjectMap properties = vertex.getValue();//                if (properties.containsKey(Utils.VERTEX_AGG_SIM_VALUE)) {//                  if ((double) properties.get(Utils.VERTEX_AGG_SIM_VALUE) < 0.6) {//                    return true;//                  }//                }//                return false;//              }//            });//        aggVertexFilter.map(new MapFunction<Vertex<Long,ObjectMap>, Tuple2<Long, Integer>>() {//          @Override//          public Tuple2<Long, Integer> map(Vertex<Long, ObjectMap> vertex) throws Exception {//            return new Tuple2<>((long) vertex.getValue().get(Utils.CC_ID), 1);//          }//        }).groupBy(0).aggregate(Aggregations.SUM, 1).print();//        System.out.println("eddges #########################");////        aggVertexSimGraph.getEdges().filter(new FilterFunction<Edge<Long, ObjectMap>>() {//          List<Long> edgeIds = Arrays.asList(414L, 1974L, 413L, 1213L, 6502L, 7490L, 6501L);//          @Override//          public boolean filter(Edge<Long, ObjectMap> edge) throws Exception {//            return edgeIds.contains(edge.getSource()) || edgeIds.contains(edge.getTarget());//          }//        }).print();//        System.out.println("v1 #########################");//        round1.getVertices().filter(new FilterFunction<Vertex<Long, ObjectMap>>() {//          @Override//          public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {//            return vertex.getValue().containsKey(Utils.CC_ID) && (//                (long) vertex.getValue().get(Utils.CC_ID) == 4744L ||//                    (long) vertex.getValue().get(Utils.CC_ID) == 6501L ||//                    (long) vertex.getValue().get(Utils.CC_ID) == 413L);//          }//        }).print();//        System.out.println("v2 #########################");//        round2.getVertices().filter(new FilterFunction<Vertex<Long, ObjectMap>>() {//          @Override//          public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {//            return vertex.getValue().containsKey(Utils.CC_ID) && (//                (long) vertex.getValue().get(Utils.CC_ID) == 4744L ||//                    (long) vertex.getValue().get(Utils.CC_ID) == 6501L ||//                    (long) vertex.getValue().get(Utils.CC_ID) == 413L);//          }//        }).print();////        LOG.info(lowSimVertices1.count());//        LOG.info(lowSimVertices2.count());        //TODO//        printEdgesSimValueBelowThreshold(allEdgesGraph, accumulatedSimValues);      }      return;    }    /**     * 4. Determination of cluster representative     * - currently: entity from best "data source" (GeoNames > DBpedia > others)     *///    final DataSet<Vertex<Long, ObjectMap>> mergedCluster = graph.getVertices()//        .groupBy(new CcIdKeySelector())//        .reduceGroup(new BestDataSourceAllLabelsGroupReduceFunction());////    if (PRINT_STATS) {////      Stats.countPrintResourcesPerCc(components);//      Stats.printLabelsForMergedClusters(mergedCluster);//    }    /**     * 5. Cluster Refinement     *///    mergedCluster.filter(new FilterFunction<Vertex<Long, ObjectMap>>() {//      @Override//      public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {//         return !(vertex.getValue().get(Utils.CL_VERTICES) instanceof Set);//      }//    });////    DataSet<Edge<Long, Double>> edgesCrossedClusters = mergedCluster//        .cross(mergedCluster)//        .with(new ClusterEdgeCreationCrossFunction())//        .filter(new FilterFunction<Edge<Long, Double>>() {//          @Override//          public boolean filter(Edge<Long, Double> edge) throws Exception {//            return edge.getValue() > 0.7;//          }//        });//    if (PRINT_STATS) {//      edgesCrossedClusters.print();////      System.out.println(edgesCrossedClusters.count());//    }  }  private static void writeEdgesToLog(Graph<Long, ObjectMap, ObjectMap> oneIterationGraph,                                      List<Long> clusterStats) throws Exception {    oneIterationGraph.filterOnEdges(new ResultEdgesSelectionFilter(clusterStats))        .getEdges().collect();  }  private static void writeVerticesToLog(Graph<Long, ObjectMap, ObjectMap> graph,                                         List<Long> clusterList) throws Exception {    graph.filterOnVertices(new ResultVerticesSelectionFilter(clusterList))        .getVertices().collect();  }  private static void writeCcToLog(Graph<Long, ObjectMap, ObjectMap> graph,                                   List<Long> clusterList) throws Exception {    graph.filterOnVertices(new ResultComponentSelectionFilter(clusterList))        .getVertices().collect();  }  private static DataSet<Edge<Long, ObjectMap>> addSimilaritiesToEdges(Graph<Long, ObjectMap, NullValue> preprocGraph) {    LOG.info("Start computing edge similarities...");    final DataSet<Triplet<Long, ObjectMap, ObjectMap>> accumulatedSimValueTriplets        = computeSimilarities(preprocGraph.getTriplets(), PRE_CLUSTER_STRATEGY);    LOG.info("Done.");    return accumulatedSimValueTriplets        .map(new TripletToEdgeMapFunction())        .map(new AggSimValueEdgeMapFunction());  }  private static Graph<Long, ObjectMap, NullValue> addCcIdsToGraph(Graph<Long, ObjectMap, NullValue> preprocGraph) throws Exception {    final DataSet<Long> ccInputVertices = preprocGraph.getVertices().map(new CcVerticesCreator());    final DataSet<Tuple2<Long, Long>> components = FlinkConnectedComponents        .compute(ccInputVertices, preprocGraph.getEdgeIds(), 1000);    return preprocGraph        .joinWithVertices(components, new CcIdVertexJoinFunction());  }  private static Graph<Long, ObjectMap, NullValue> createAllEdgesGraph(      Graph<Long, ObjectMap, NullValue> graph) throws Exception {    final DataSet<Edge<Long, NullValue>> allEdges = ClusterComputation        .computeComponentEdges(addCcIdsToGraph(graph).getVertices(), true);    return Graph.fromDataSet(graph.getVertices(), allEdges, env);  }  public static DataSet<Triplet<Long, ObjectMap, ObjectMap>> computeSimilarities(DataSet<Triplet<Long, ObjectMap,      NullValue>> triplets, String filter) {    LOG.info("Started: compute similarities...");    switch (filter) {      case "geo":        return basicGeoSimilarity(triplets);      case "label":        return basicTrigramSimilarity(triplets);      case "type":        return basicTypeSimilarity(triplets);      default:        return joinDifferentSimilarityValues(basicGeoSimilarity(triplets),            basicTrigramSimilarity(triplets),            basicTypeSimilarity(triplets));    }  }  /**   * Join several sets of triplets which are being produced within property similarity computation.   * Edges where no similarity value is higher than the appropriate threshold are not in the result set.   * @param tripletDataSet intput datasets   * @return joined dataset with all similarities in an ObjectMap   */  @SafeVarargs  private static DataSet<Triplet<Long, ObjectMap, ObjectMap>> joinDifferentSimilarityValues(      DataSet<Triplet<Long, ObjectMap, ObjectMap>>... tripletDataSet) {    DataSet<Triplet<Long, ObjectMap, ObjectMap>> triplets = null;    boolean isFirstSet = false;    for (DataSet<Triplet<Long, ObjectMap, ObjectMap>> dataSet : tripletDataSet) {      if (!isFirstSet) {        triplets = dataSet;        isFirstSet = true;      } else {        triplets = triplets            .fullOuterJoin(dataSet)            .where(0, 1)            .equalTo(0, 1)            .with(new FullOuterJoinSimilarityValueFunction());      }    }    return triplets;  }  private static DataSet<Triplet<Long, ObjectMap, ObjectMap>> basicTypeSimilarity(      DataSet<Triplet<Long, ObjectMap, NullValue>> triplets) {    return triplets        .map(new TypeSimilarityMapper());  }  private static DataSet<Triplet<Long, ObjectMap, ObjectMap>> basicTrigramSimilarity(      DataSet<Triplet<Long, ObjectMap, NullValue>> triplets) {    return triplets        .map(new TrigramSimilarityMapper());  }  private static DataSet<Triplet<Long, ObjectMap, ObjectMap>> basicGeoSimilarity(      DataSet<Triplet<Long, ObjectMap, NullValue>> triplets) {    return triplets        .filter(new EmptyGeoCodeFilter())        .map(new GeoCodeSimMapper());  }  /**   * Create the input graph for further analysis,   * restrict to edges where source and target are in vertices set.   * @return graph with vertices and edges.   * @throws Exception   * @param fullDbString complete server+port+db string   */  public static Graph<Long, ObjectMap, NullValue> getInputGraph(String fullDbString)      throws Exception {    JDBCDataLoader loader = new JDBCDataLoader(env);    DataSet<Vertex<Long, ObjectMap>> vertices = loader.getVertices(fullDbString);    // restrict edges to these where source and target are vertices    DataSet<Edge<Long, NullValue>> edges = loader.getEdges(fullDbString)        .leftOuterJoin(vertices)        .where(0).equalTo(0)        .with(new EdgeRestrictFlatJoinFunction())        .leftOuterJoin(vertices)        .where(1).equalTo(0)        .with(new EdgeRestrictFlatJoinFunction());    // delete vertices without any edges due to restriction    DataSet<Vertex<Long, ObjectMap>> left = vertices        .leftOuterJoin(edges)        .where(0).equalTo(0)        .with(new VertexRestrictFlatJoinFunction()).distinct(0);    DataSet<Vertex<Long, ObjectMap>> finalVertices = vertices        .leftOuterJoin(edges)        .where(0).equalTo(1)        .with(new VertexRestrictFlatJoinFunction()).distinct(0)        .union(left);    return Graph.fromDataSet(finalVertices, edges, env);  }  /**   * Parses the program arguments or returns help if args are empty.   *   * @param args program arguments   * @return command line which can be used in the program   */  private static CommandLine parseArguments(String[] args) throws ParseException {    if (args.length == 0) {      HelpFormatter formatter = new HelpFormatter();      formatter.printHelp(MappingAnalysisExample.class.getName(), OPTIONS, true);      return null;    }    CommandLineParser parser = new BasicParser();    return parser.parse(OPTIONS, args);  }  @Override  public String getDescription() {    return MappingAnalysisExample.class.getName();  }  public static class AddAggSimVertexJoinFunction implements JoinFunction<Vertex<Long, ObjectMap>, Tuple2<Long, Double>, Vertex<Long, ObjectMap>> {    @Override    public Vertex<Long, ObjectMap> join(Vertex<Long, ObjectMap> vertex, Tuple2<Long, Double> tuple) throws Exception {      vertex.getValue().put(Utils.VERTEX_AGG_SIM_VALUE, tuple.f1);      return vertex;    }  }  public static class LowSimFilterFunction implements FilterFunction<Vertex<Long, ObjectMap>> {    private final double threshold;    public LowSimFilterFunction(double t) {      this.threshold = t;    }    @Override    public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {      return (double) vertex.getValue().get(Utils.VERTEX_AGG_SIM_VALUE) < threshold;    }  }  //  private static class ChangeCcIdMapFunction implements MapFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {//    @Override//    public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {//      ObjectMap value = vertex.getValue();//      value.put(Utils.CC_ID, value.get(Utils.CC_ID) + maxCcId);//      return vertex;//    }//  }}