package org.mappinganalysis;import com.google.common.base.Preconditions;import com.google.common.collect.Lists;import com.google.common.collect.Sets;import com.google.common.primitives.Doubles;import org.apache.commons.cli.*;import org.apache.flink.api.common.ProgramDescription;import org.apache.flink.api.common.functions.FilterFunction;import org.apache.flink.api.common.functions.FlatJoinFunction;import org.apache.flink.api.common.functions.FlatMapFunction;import org.apache.flink.api.common.functions.MapFunction;import org.apache.flink.api.java.DataSet;import org.apache.flink.api.java.ExecutionEnvironment;import org.apache.flink.api.java.operators.AggregateOperator;import org.apache.flink.api.java.operators.FlatMapOperator;import org.apache.flink.api.java.operators.JoinOperator;import org.apache.flink.api.java.operators.MapOperator;import org.apache.flink.api.java.tuple.Tuple1;import org.apache.flink.api.java.tuple.Tuple2;import org.apache.flink.graph.Edge;import org.apache.flink.graph.Graph;import org.apache.flink.graph.Vertex;import org.apache.flink.types.NullValue;import org.apache.flink.util.Collector;import org.apache.log4j.Logger;import org.mappinganalysis.io.output.ExampleOutput;import org.mappinganalysis.model.ObjectMap;import org.mappinganalysis.model.Preprocessing;import org.mappinganalysis.model.functions.refinement.Refinement;import org.mappinganalysis.model.functions.representative.MajorityPropertiesGroupReduceFunction;import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;import org.mappinganalysis.utils.Utils;import org.mappinganalysis.utils.functions.filter.ClusterSizeSimpleFilterFunction;import org.mappinganalysis.utils.functions.keyselector.HashCcIdKeySelector;import java.util.ArrayList;import java.util.Set;/** * Mapping analysis example */public class MappingAnalysisExample implements ProgramDescription {  private static final Logger LOG = Logger.getLogger(MappingAnalysisExample.class);  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();  /**   * @param args cmd args   * @throws Exception   */  public static void main(String[] args) throws Exception {    Preconditions.checkArgument(        args.length == 5, "args[0]: choose dataset (linklion or geo), args[1]: input dir, " +            "args[2]: verbosity(less, info, debug), args[3]: isSimSortEnabled (true, false), " +            "args[4]: minimum Similarity (e.g., 0.7)");    CommandLine cmd = parseArguments(args);    String dataset;    final String optionDataset = args[0];    Utils.INPUT_DIR = args[1];    Utils.VERBOSITY = args[2];    Utils.IS_SIMSORT_ENABLED = args[3].equals("true");    Utils.MIN_SIM = Doubles.tryParse(args[4]);    switch (optionDataset) {      case Utils.CMD_LL:        dataset = Utils.LL_FULL_NAME;        break;      case Utils.CMD_GEO:        dataset = Utils.GEO_FULL_NAME;        break;      default:        return;    }    Utils.IS_LINK_FILTER_ACTIVE = cmd.hasOption(OPTION_LINK_FILTER_PREPROCESSING);    Utils.IGNORE_MISSING_PROPERTIES = cmd.hasOption(OPTION_IGNORE_MISSING_PROPERTIES);    Utils.PRE_CLUSTER_STRATEGY = cmd.getOptionValue(OPTION_PRE_CLUSTER_FILTER, Utils.DEFAULT_VALUE);    final Graph<Long, ObjectMap, NullValue> graph = Preprocessing.getInputGraphFromCsv(env);    execute(graph, dataset);  }  /**   * Mapping analysis computation   */  private static void execute(Graph<Long, ObjectMap, NullValue> inputGraph, String dataset) throws Exception {    ExampleOutput out = new ExampleOutput(env);//    Preprocessing.deleteVerticesWithoutAnyEdges()////    addCountsForSingleSource(inputGraph, out, Utils.GN_NS);//    addCountsForSingleSource(inputGraph, out, Utils.DBP_NS);//    addCountsForSingleSource(inputGraph, out, Utils.FB_NS);////    out.print();////    if (true) {//      return;//    }//    ArrayList<Long> clusterList = Lists.newArrayList(1458L);//, 2913L);//, 4966L, 5678L);    final ArrayList<Long> vertexList;    if (dataset.equals(Utils.GEO_FULL_NAME)) {      vertexList = Lists.newArrayList(1827L, 5026L, 6932L, 3420L, 5586L, 3490L, 3419L);//123L, 122L, 2060L, 1181L);    } else {      vertexList = Lists.newArrayList(100972L, 121545L, 276947L, 235633L, 185488L, 100971L, 235632L, 121544L, 909033L);    }    Utils.IGNORE_MISSING_PROPERTIES = true;    Utils.IS_LINK_FILTER_ACTIVE = true;    out.addVertexAndEdgeSizes("0 vertex and edge sizes input graph", inputGraph);    final Graph<Long, ObjectMap, ObjectMap> preprocGraph = Preprocessing        .execute(inputGraph, Utils.IS_LINK_FILTER_ACTIVE, env, out); // restrict graph here    out.addVertexAndEdgeSizes("2 vertex and edge sizes post preprocessing", preprocGraph);    out.addPreClusterSizes("2 cluster sizes post preprocessing", preprocGraph.getVertices(), Utils.CC_ID);    Utils.writeToHdfs(preprocGraph.getVertices(), "2_post_preprocessing");    Graph<Long, ObjectMap, ObjectMap> graph = SimilarityComputation        .executeAdvanced(preprocGraph, Utils.DEFAULT_VALUE, env, out);    Utils.writeToHdfs(graph.getVertices(), "4_post_sim_sort");    out.addVertexAndEdgeSizes("4 vertex and edge sizes post simsort", graph);//    /* 4. Representative */    DataSet<Vertex<Long, ObjectMap>> representativeVertices = graph.getVertices()        .groupBy(new HashCcIdKeySelector())        .reduceGroup(new MajorityPropertiesGroupReduceFunction());    Utils.writeToHdfs(representativeVertices, "5_cluster_representatives");    out.addClusterSizes("5 cluster sizes representatives", representativeVertices);//    /* 5. Refinement */    representativeVertices = Refinement.init(representativeVertices);    out.addClusterSizes("6a cluster sizes merge init", representativeVertices);    DataSet<Vertex<Long, ObjectMap>> mergedClusters = Refinement.execute(representativeVertices, out);    DataSet<Tuple2<Long, Long>> allResultEdgeIds = mergedClusters        .flatMap(new FlatMapFunction<Vertex<Long,ObjectMap>, Tuple2<Long, Long>>() {          @Override          public void flatMap(Vertex<Long, ObjectMap> vertex, Collector<Tuple2<Long, Long>> collector) throws Exception {            Set<Long> leftList = Sets.newHashSet(vertex.getValue().getVerticesList());            Set<Long> rightList = Sets.newHashSet(leftList);            for (Long left : leftList) {              rightList.remove(left);              for (Long right : rightList) {                if (left < right) {                  collector.collect(new Tuple2<>(left, right));                } else {                  collector.collect(new Tuple2<>(right, left));                }              }            }          }        });    out.addDataSetCount("all result edges count", allResultEdgeIds);    DataSet<Tuple2<Long, Long>> inputEdgeIds = inputGraph        .getEdgeIds()        .map(new MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {          @Override          public Tuple2<Long, Long> map(Tuple2<Long, Long> tuple) throws Exception {            if (tuple.f0 < tuple.f1) {              return tuple;            } else {              return new Tuple2<>(tuple.f1, tuple.f0);            }          }        });    DataSet<Tuple2<Integer, Integer>> newPlusDeletedEdges = allResultEdgeIds.fullOuterJoin(inputEdgeIds)        .where(0, 1)        .equalTo(0, 1)        .with(new FlatJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Integer, Integer>>() {          @Override          public void join(Tuple2<Long, Long> left, Tuple2<Long, Long> right,                           Collector<Tuple2<Integer, Integer>> collector) throws Exception {            if (left == null) {              collector.collect(new Tuple2<>(0, 1));            }            if (right == null) {              collector.collect(new Tuple2<>(1, 0));            }          }        }).sum(0).andSum(1);    out.addTuples("new edges and deleted edges", newPlusDeletedEdges);    Utils.writeToHdfs(mergedClusters, "6 mergedVertices");    out.addClusterSizes("6b cluster sizes merged", mergedClusters);//    out.printEvalThreePercent("3% eval", mergedClusters, preprocGraph.getVertices());////    DataSet<Vertex<Long, ObjectMap>> changedWhileMerging = representativeVertices//        .filter(new ClusterSizeSimpleFilterFunction(4))//        .rightOuterJoin(mergedClusters.filter(new ClusterSizeSimpleFilterFunction(4)))//        .where(0)//        .equalTo(0)//        .with(new FlatJoinFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>,//            Vertex<Long, ObjectMap>>() {//          @Override//          public void join(Vertex<Long, ObjectMap> left, Vertex<Long, ObjectMap> right,//                           Collector<Vertex<Long, ObjectMap>> collector) throws Exception {//            if (left == null) {//              collector.collect(right);//            }//          }//        });//////    out.addRandomBaseClusters("random base clusters", preprocGraph.getVertices(), changedWhileMerging, 10);////    out.addVertices("changedWhileMerging", changedWhileMerging);//   todo out.printClusterAndOriginalVertices("Changed clusters and the contained original vertices",//        changedWhileMerging, preprocGraph.getVertices());////    out.addRandomFinalClustersWithMinSize(////        "final random clusters with vertex properties from preprocessing",////        representativeVertices,////        graph.getVertices(),////        15, 20);////////    out.addRandomBaseClusters("base random clusters with vertex properties from final",////        graph.getVertices(),////        representativeVertices, 20);//////    if (dataset.equals(Utils.GEO_FULL_NAME)) {////      final ArrayList<Long> preSplitBigClusterList////          = Lists.newArrayList(4794L, 28L, 3614L, 4422L, 1429L, 1458L, 1868L);////      out.addSelectedBaseClusters("big components final values",////          Preprocessing.execute(inputGraph, true, env).getVertices(),////          representativeVertices,////          preSplitBigClusterList);////    }////    out.addSelectedVertices("end vertices", representativeVertices, vertexList);    out.print();//        Stats.printAccumulatorValues(env, graph, simSortKeySelector);//        Stats.printComponentSizeAndCount(graph.getVertices());//        Stats.countPrintGeoPointsPerOntology(preprocGraph);//        printEdgesSimValueBelowThreshold(allEdgesGraph, accumulatedSimValues);//    env.execute();//    JobExecutionResult jobExecResult = env.getLastJobExecutionResult();////    LOG.info("[1] ### BaseVertexCreator vertex counter: "//        + jobExecResult.getAccumulatorResult(Utils.BASE_VERTEX_COUNT_ACCUMULATOR));//    LOG.info("[1] ### PropertyCoGroupFunction vertex counter: "//        + jobExecResult.getAccumulatorResult(Utils.VERTEX_COUNT_ACCUMULATOR));//    LOG.info("[1] ### FlinkEdgeCreator edge counter: "//        + jobExecResult.getAccumulatorResult(Utils.EDGE_COUNT_ACCUMULATOR));//    LOG.info("[1] ### FlinkPropertyMapper property counter: "//        + jobExecResult.getAccumulatorResult(Utils.PROP_COUNT_ACCUMULATOR));//    LOG.info("[1] ### typeMismatchCorrection wrong edges counter: "//        + jobExecResult.getAccumulatorResult(Utils.EDGE_EXCLUDE_ACCUMULATOR));//    LOG.info("[1] ### applyLinkFilterStrategy correct edges counter: "//        + jobExecResult.getAccumulatorResult(Utils.LINK_FILTER_ACCUMULATOR));////    LOG.info("[3] ### Representatives created: "//        + jobExecResult.getAccumulatorResult(Utils.REPRESENTATIVE_ACCUMULATOR)); // MajorityPropertiesGRFunction//    LOG.info("[3] ### Clusters created in refinement step: "//        + jobExecResult.getAccumulatorResult(Utils.REFINEMENT_MERGE_ACCUMULATOR)); // SimilarClusterMergeMapFunction//    LOG.info("[3] ### Excluded vertex counter: "//        + jobExecResult.getAccumulatorResult(Utils.EXCLUDE_VERTEX_ACCUMULATOR)); // ExcludeVertexFlatJoinFunction  }  /**   * only working for small geo dataset   */  private static void addCountsForSingleSource(Graph<Long, ObjectMap, NullValue> inputGraph,                                               ExampleOutput out, final String source) {    DataSet<Vertex<Long, ObjectMap>> nytVertices = inputGraph.getVertices()        .filter(new FilterFunction<Vertex<Long, ObjectMap>>() {      @Override      public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {        return vertex.getValue().get(Utils.ONTOLOGY).toString().equals(Utils.NYT_NS);      }    });    DataSet<Vertex<Long, ObjectMap>> gnVertices = inputGraph.getVertices()        .filter(new FilterFunction<Vertex<Long, ObjectMap>>() {      @Override      public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {        return vertex.getValue().get(Utils.ONTOLOGY).toString().equals(source);      }    });    DataSet<Edge<Long, NullValue>> edgeDataSet = Preprocessing        .deleteEdgesWithoutSourceOrTarget(inputGraph, nytVertices.union(gnVertices));    out.addDataSetCount("vertex size for " + source, gnVertices);    out.addDataSetCount("edge size for " + source, edgeDataSet);  }  /**   * Parses the program arguments or returns help if args are empty.   *   * @param args program arguments   * @return command line which can be used in the program   */  private static CommandLine parseArguments(String[] args) throws ParseException {    CommandLineParser parser = new BasicParser();    return parser.parse(OPTIONS, args);  }  /**   * Command line options   */  private static final String OPTION_LINK_FILTER_PREPROCESSING = "lfp";  private static final String OPTION_PRE_CLUSTER_FILTER = "pcf";  private static final String OPTION_ONLY_INITIAL_CLUSTER = "oic";  private static final String OPTION_DATA_SET_NAME = "ds";  private static final String OPTION_WRITE_STATS = "ws";  private static final String OPTION_CLUSTER_STATS = "cs";  private static final String OPTION_IGNORE_MISSING_PROPERTIES = "imp";  private static final String OPTION_PROCESSING_MODE = "pm";  private static final String OPTION_TYPE_MISS_MATCH_CORRECTION = "tmmc";  private static Options OPTIONS;  static {    OPTIONS = new Options();    // general    OPTIONS.addOption(OPTION_DATA_SET_NAME, "dataset-name", true,        "Choose one of the datasets [" + Utils.CMD_GEO + " (default), " + Utils.CMD_LL + "].");    OPTIONS.addOption(OPTION_PROCESSING_MODE, "processing-mode", true,        "Choose the processing mode [SimSort + TypeGroupBy (default), simSortOnly].");    OPTIONS.addOption(OPTION_IGNORE_MISSING_PROPERTIES, "ignore-missing-properties", false,        "Do not penalize missing properties on resources in similarity computation process (default: false).");    OPTIONS.addOption(OPTION_ONLY_INITIAL_CLUSTER, "only-initial-cluster", false,        "Don't compute final clusters, stop after preprocessing (default: false).");    // Preprocessing    OPTIONS.addOption(OPTION_LINK_FILTER_PREPROCESSING, "link-filter-preprocessing", false,        "Exclude edges where vertex has several target vertices having equal dataset ontology (default: false).");    OPTIONS.addOption(OPTION_TYPE_MISS_MATCH_CORRECTION, "type-miss-match-correction", false,        "Exclude edges where directly connected source and target vertices have different type property values. " +            "(default: false).");    // todo to be changed    OPTIONS.addOption(OPTION_PRE_CLUSTER_FILTER, "pre-cluster-filter", true,        "Specify preprocessing filter strategy for entity properties ["            + Utils.DEFAULT_VALUE + " (combined), geo, label, type]");    OPTIONS.addOption(OPTION_WRITE_STATS, "write-stats", false,        "Write statistics to output (default: false).");    Option clusterStats = new Option(OPTION_CLUSTER_STATS, "cluster-stats", true,        "Be more verbose while processing specified cluster ids.");    clusterStats.setArgs(Option.UNLIMITED_VALUES);    OPTIONS.addOption(clusterStats);  }  @Override  public String getDescription() {    return MappingAnalysisExample.class.getName();  }}