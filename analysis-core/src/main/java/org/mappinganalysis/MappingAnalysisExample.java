package org.mappinganalysis;import com.google.common.base.Preconditions;import com.google.common.primitives.Doubles;import org.apache.flink.api.common.ProgramDescription;import org.apache.flink.api.common.typeinfo.TypeHint;import org.apache.flink.api.java.DataSet;import org.apache.flink.api.java.ExecutionEnvironment;import org.apache.flink.api.java.tuple.Tuple2;import org.apache.flink.graph.Graph;import org.apache.flink.graph.Vertex;import org.apache.flink.graph.library.GSAConnectedComponents;import org.apache.flink.types.NullValue;import org.apache.log4j.Logger;import org.mappinganalysis.io.impl.csv.CSVDataSource;import org.mappinganalysis.io.impl.json.JSONDataSink;import org.mappinganalysis.io.impl.json.JSONDataSource;import org.mappinganalysis.io.output.ExampleOutput;import org.mappinganalysis.model.ObjectMap;import org.mappinganalysis.model.functions.decomposition.Clustering;import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreator;import org.mappinganalysis.model.functions.decomposition.simsort.SimSort;import org.mappinganalysis.model.functions.decomposition.typegroupby.TypeGroupBy;import org.mappinganalysis.model.functions.merge.MergeExecution;import org.mappinganalysis.model.functions.merge.MergeInitialization;import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;import org.mappinganalysis.model.functions.preprocessing.LinkFilter;import org.mappinganalysis.model.impl.LinkFilterStrategy;import org.mappinganalysis.util.Constants;import org.mappinganalysis.util.Stats;/** * Mapping analysis example */public class MappingAnalysisExample implements ProgramDescription {  private static final Logger LOG = Logger.getLogger(MappingAnalysisExample.class);  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();  static Double minSimilarity;  /**   * @param args cmd args   */  public static void main(String[] args) throws Exception {    Preconditions.checkArgument(        args.length == 6, "args[0]: input dir, " +            "args[1]: verbosity(less, info, debug), " +            "args[2]: isSimSortEnabled (isSimsort), " +            "args[3]: min SimSort similarity (e.g., 0.7), " +            "args[4]: linklion mode (all, nyt, random)" +            "args[5]: processing mode (input, preproc, analysis, eval)");    Constants.INPUT_DIR = args[0];    Constants.SOURCE_COUNT = args[0].contains("linklion") ? 5 : 4;    Constants.VERBOSITY = args[1];    Constants.IS_SIMSORT_ENABLED = args[2].equals("isSimsort");//    Constants.IS_SIMSORT_ALT_ENABLED = args[2].equals("isSimsortAlt");    minSimilarity = Doubles.tryParse(args[3]);    Constants.LL_MODE = args[4];    Constants.PROC_MODE = args[5];    ExampleOutput out = new ExampleOutput(env);    switch (Constants.PROC_MODE) {      case Constants.READ_INPUT:        CSVDataSource.createInputGraphFromCsv(env, out);        break;      case Constants.PREPROC:        executePreprocessing(out);        break;      case Constants.LF:        executeLf(out);        break;      case Constants.PREPROCESSING_COMPLETE: // combination of preproc and lf        executeCompletePreprocessing(out);        break;      case Constants.IC:        executeIc(out);        break;      case Constants.DECOMP:        executeDecompositionOne(out);        break;      case Constants.SIMSORT: // without prep        executeSimSort(out);        break;      case Constants.DECOMPOSITION_COMPLETE: // combination of ic, decomp and simsort        executeCompleteDecomposition(out);        break;      case Constants.ANALYSIS:        executeReprCreationAndMerge(out);        break;      case Constants.ALL://        Preprocessing.createInputGraphFromCsv(env, out);        executePreprocessing(out);        executeLf(out);        executeIc(out);        executeDecompositionOne(out);        executeSimSort(out);        executeReprCreationAndMerge(out);        break;      case Constants.EVAL:        executeEval(out);        break;      case Constants.STATS_EDGE_INPUT:        executeStats(Constants.LL_MODE.concat(Constants.INPUT_GRAPH), "edge");        break;      case Constants.STATS_EDGE_PREPROC:        executeStats(Constants.INIT_CLUST, "edge");        break;      case Constants.STATS_VERTEX_INPUT:        executeStats(Constants.LL_MODE.concat(Constants.INPUT_GRAPH), "vertex");        break;      case Constants.STATS_VERTEX_PREPROC:        executeStats(Constants.INIT_CLUST, "vertex");        break;      case Constants.MISS:        executeStats(Constants.LL_MODE.concat(Constants.INPUT_GRAPH), "-");        break;      case Constants.TEST:        executeTest();        break;      case Constants.COMPLETE:        executeComplete(out);        break;    }  }  /**   * Test workflow, changing TODO WRITE TEST   * not currently working   */  private static void executeTest() throws Exception {    // prepared graph with Long Long NV    DataSet<Tuple2<Long, Long>> vertexTuples =        new JSONDataSource(Constants.INPUT_DIR,            Constants.LL_MODE.concat("CcInput"), env)        .getGraph(Long.class, NullValue.class)        .run(new GSAConnectedComponents<>(1000))        .map(vertex -> new Tuple2<>(vertex.getId(), vertex.getValue()))        .returns(new TypeHint<Tuple2<Long, Long>>() {});    new JSONDataSink(Constants.INPUT_DIR, Constants.LL_MODE.concat("ccResult"))        .writeTuples(vertexTuples);    env.execute();  }  /**   * execute preprocessing   */  private static void executePreprocessing(ExampleOutput out) throws Exception {    Graph<Long, ObjectMap, NullValue> inGraph =        new JSONDataSource(Constants.INPUT_DIR,            Constants.LL_MODE.concat(Constants.INPUT_GRAPH), env)            .getGraph(ObjectMap.class, NullValue.class);    if (Constants.VERBOSITY.equals(Constants.DEBUG)) {      out.addVertexAndEdgeSizes("input size", inGraph);    }    Graph<Long, ObjectMap, ObjectMap> graph        = inGraph.run(new DefaultPreprocessing(env));    new JSONDataSink(Constants.INPUT_DIR, Constants.LL_MODE.concat(Constants.PREPROC_GRAPH))        .writeGraph(graph);    printLogOrExecute(out, "Basic Preprocessing");  }  /**   * LinkFilter - Preprocessing strategy to restrict resources to have only one   * counterpart in every target ontology   */  private static void executeLf(ExampleOutput out) throws Exception {    LinkFilter linkFilter = new LinkFilter        .LinkFilterBuilder()        .setEnvironment(env)        .setRemoveIsolatedVertices(true)        .setStrategy(LinkFilterStrategy.BASIC)        .build();    Graph<Long, ObjectMap, ObjectMap> graph =        new JSONDataSource(Constants.INPUT_DIR,            Constants.LL_MODE.concat(Constants.PREPROC_GRAPH), env)            .getGraph()            .run(linkFilter);    new JSONDataSink(Constants.INPUT_DIR, Constants.LL_MODE.concat(Constants.LF_GRAPH))        .writeGraph(graph);    env.execute("Execute link filter strategy");  }  /**   * Preprocessing + LinkFilter together   */  private static void executeCompletePreprocessing(ExampleOutput out) throws Exception {    Graph<Long, ObjectMap, ObjectMap> graph        = new JSONDataSource(Constants.INPUT_DIR,        Constants.LL_MODE.concat(Constants.INPUT_GRAPH), env)        .getGraph(ObjectMap.class, NullValue.class)        .run(new DefaultPreprocessing(true, env));    new JSONDataSink(Constants.INPUT_DIR, Constants.LL_MODE.concat(Constants.LF_GRAPH))        .writeGraph(graph);    env.execute("Preprocessing");  }  /**   * InitialClutering   */  private static void executeIc(ExampleOutput out) throws Exception {    Graph<Long, ObjectMap, ObjectMap> graph =        new JSONDataSource(Constants.INPUT_DIR,            Constants.LL_MODE.concat(Constants.LF_GRAPH), env)            .getGraph();    graph = Clustering.createInitialClustering(graph, Constants.VERBOSITY, out, env);    new JSONDataSink(Constants.INPUT_DIR, Constants.LL_MODE.concat(Constants.IC_GRAPH))        .writeGraph(graph);    printLogOrExecute(out, "Initial Clustering");  }  /**   * First part decomposition, TypeGroupBy and preparation phase of SimSort   */  private static void executeDecompositionOne(ExampleOutput out) throws Exception {    Graph<Long, ObjectMap, ObjectMap> graph =        new JSONDataSource(Constants.INPUT_DIR,            Constants.LL_MODE.concat(Constants.IC_GRAPH), env)            .getGraph();    graph = graph.run(new TypeGroupBy(env));    /*     * SimSort (and postprocessing of TypeGroupBy in prepare)     */    graph = SimSort.prepare(graph, env);    if (Constants.VERBOSITY.equals(Constants.DEBUG)) {      out.addPreClusterSizes("3 cluster sizes post typegroupby",          graph.getVertices(), Constants.HASH_CC);    }    new JSONDataSink(Constants.INPUT_DIR, Constants.LL_MODE.concat(Constants.DECOMP_GRAPH))        .writeGraph(graph);    printLogOrExecute(out, "Decomposition Part 1");  }  private static void executeSimSort(ExampleOutput out) throws Exception {    Graph<Long, ObjectMap, ObjectMap> graph =        new JSONDataSource(Constants.INPUT_DIR,            Constants.LL_MODE.concat(Constants.DECOMP_GRAPH), env)            .getGraph()            .run(new SimSort(false, minSimilarity, env));    if (Constants.VERBOSITY.equals(Constants.DEBUG)) {      out.addVertexAndEdgeSizes("4 vertex and edge sizes post decomposition", graph);    }    new JSONDataSink(Constants.INPUT_DIR, Constants.LL_MODE.concat(Constants.SIMSORT_GRAPH))        .writeGraph(graph);    printLogOrExecute(out, "SimSort");  }  /**   * Initical Clustering, TypeGroupBy and SimSort   */  private static void executeCompleteDecomposition(ExampleOutput out) throws Exception {    Graph<Long, ObjectMap, ObjectMap> graph =        new JSONDataSource(Constants.INPUT_DIR,            Constants.LL_MODE.concat(Constants.LF_GRAPH), env)            .getGraph();    graph = Clustering        .createInitialClustering(graph, Constants.VERBOSITY, out, env)        .run(new TypeGroupBy(env))        .run(new SimSort(minSimilarity, env));//    if (Constants.VERBOSITY.equals(Constants.DEBUG)) {//      out.addVertexAndEdgeSizes("4 vertex and edge sizes post decomposition", graph);//    }    new JSONDataSink(Constants.INPUT_DIR, Constants.LL_MODE.concat(Constants.SIMSORT_GRAPH))        .writeGraph(graph);    env.execute("Decomposition");  }  private static void executeReprCreationAndMerge(ExampleOutput out) throws Exception {    Graph<Long, ObjectMap, ObjectMap> graph =        new JSONDataSource(Constants.INPUT_DIR,            Constants.LL_MODE.concat(Constants.SIMSORT_GRAPH), env)            .getGraph();    /* 4. Representative */    DataSet<Vertex<Long, ObjectMap>> representatives = graph.getVertices()        .runOperation(new RepresentativeCreator());    if (Constants.VERBOSITY.equals(Constants.DEBUG)) {      out.addClusterSizes("5 cluster sizes representatives", representatives);    }    new JSONDataSink(Constants.INPUT_DIR, "5-representatives-json")        .writeVertices(representatives);//    Utils.writeVerticesToJSONFile(representatives, "5-representatives-json");    /* 5. Merge */    representatives = representatives        .runOperation(new MergeInitialization())        .runOperation(new MergeExecution(Constants.SOURCE_COUNT));    new JSONDataSink(Constants.INPUT_DIR, "6-merged-clusters-json")        .writeVertices(representatives);    if (Constants.VERBOSITY.equals(Constants.DEBUG)) {      out.addClusterSizes("6b cluster sizes merged", representatives);      out.print();    } else {      env.execute("Representatives and Merge");    }  }  private static void executeComplete(ExampleOutput out) throws Exception {    Graph<Long, ObjectMap, ObjectMap> graph = preprocessing();    graph = Clustering        .createInitialClustering(graph, Constants.VERBOSITY, out, env)        .run(new TypeGroupBy(env))        .run(new SimSort(minSimilarity, env));    DataSet<Vertex<Long, ObjectMap>> representatives = graph.getVertices()        .runOperation(new RepresentativeCreator());    if (Constants.VERBOSITY.equals(Constants.DEBUG)) {      out.addClusterSizes("5 cluster sizes representatives", representatives);    }//    Utils.writeVerticesToJSONFile(representatives, "5-representatives-json");    /* 5. Merge */    representatives = representatives        .runOperation(new MergeInitialization())        .runOperation(new MergeExecution(Constants.SOURCE_COUNT));    new JSONDataSink(Constants.INPUT_DIR, "6-merged-clusters-json")        .writeVertices(representatives);//    Utils.writeVerticesToJSONFile(representatives, "6-merged-clusters-json");    if (Constants.VERBOSITY.equals(Constants.DEBUG)) {      out.addClusterSizes("6b cluster sizes merged", representatives);      out.print();    } else {      env.execute("Representatives and Merge");    }  }  private static Graph<Long, ObjectMap, ObjectMap> preprocessing()      throws Exception {    return new JSONDataSource(Constants.INPUT_DIR,        Constants.LL_MODE.concat(Constants.INPUT_GRAPH), env)        .getGraph(ObjectMap.class, NullValue.class)        .run(new DefaultPreprocessing(true, env));  }  /**   * if analysis rerun, add vertices to 5 + 6 directories   * @throws Exception   */  private static void executeEval(ExampleOutput out) throws Exception {    DataSet<Vertex<Long, ObjectMap>> mergedClusters =        new JSONDataSource(Constants.INPUT_DIR,            "6-merged-clusters-json", env)            .getVertices(ObjectMap.class);//    DataSet<Vertex<Long, ObjectMap>> representativeVertices//        = Utils.readVerticesFromJSONFile("5-representatives-json", env, false);    Graph<Long, ObjectMap, ObjectMap> graph =        new JSONDataSource(Constants.INPUT_DIR,            Constants.LL_MODE.concat(Constants.PREPROC_GRAPH), env)            .getGraph();//    Stats.printResultEdgeCounts(graph.mapEdges(new MapFunction<Edge<Long, ObjectMap>, NullValue>() {//      @Override//      public NullValue map(Edge<Long, ObjectMap> value) throws Exception {//        return NullValue.getInstance();//      }//    }), out, mergedClusters);//    out.addSelectedBaseClusters("selected base clusters final values",//        graph.getVertices(),//        representativeVertices, Utils.getVertexList(dataset));    out.printEvalThreePercent("eval", mergedClusters, graph.getVertices());    out.print();//    Stats.addChangedWhileMergingVertices(out, representativeVertices, mergedClusters);  }  /**   * For vertices, get vertex count per data source.   * For edges, get edge (undirected) edge counts between sources.   */  public static void executeStats(String step, String edgeOrVertex) throws Exception {    Graph<Long, ObjectMap, ObjectMap> graph =        new JSONDataSource(Constants.INPUT_DIR, step, env).getGraph();    if (edgeOrVertex.equals("edge")) { //&& graphPath.contains("InputGraph")) {      Stats.printEdgeSourceCounts(graph)          .print();    } else if (edgeOrVertex.equals("vertex")) {      Stats.printVertexSourceCounts(graph)          .print();    } else if (Constants.PROC_MODE.equals(Constants.MISS)) {      Stats.countMissingGeoAndTypeProperties(          Constants.LL_MODE.concat(Constants.INPUT_GRAPH),          false,          env)        .print();    }//    else {//      Graph<Long, ObjectMap, ObjectMap> input//          = Utils.readFromJSONFile(Constants.LL_MODE + "InputGraph", env);////      DataSet<Edge<Long, ObjectMap>> newEdges = Preprocessing//          .deleteEdgesWithoutSourceOrTarget(input.getEdges(), graph.getVertices());//      graph = Graph.fromDataSet(graph.getVertices(), newEdges, env);//      printEdgeSourceCounts(graph);//    }  }  private static void printLogOrExecute(ExampleOutput out, String jobName) throws Exception {    if (Constants.VERBOSITY.equals(Constants.DEBUG)) {      out.print();    } else {      env.execute(jobName);    }  }  @Override  public String getDescription() {    return MappingAnalysisExample.class.getName();  }}