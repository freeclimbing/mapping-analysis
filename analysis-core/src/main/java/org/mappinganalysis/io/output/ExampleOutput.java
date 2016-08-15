package org.mappinganalysis.io.output;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.stats.FrequencyMapByFunction;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.functions.filter.ClusterSizeSimpleFilterFunction;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Logging output class. In gradoop already deprecated methods, try to avoid usage.
 */
public class ExampleOutput {
  private static final Logger LOG = Logger.getLogger(ExampleOutput.class);

  /**
   * Flink dataset, collecting the output lines
   */
  private DataSet<ArrayList<String>> outSet;
  private ExecutionEnvironment env;

  public ExampleOutput(ExecutionEnvironment env) {
    this.outSet = env.fromElements(new ArrayList<>());
    this.env = env;
  }

  /**
   * addGraph collection to output
   * @param caption output caption
   */
  public <T> void addGraph(String caption, Graph<Long, ObjectMap, T> graph) {
    DataSet<String> captionSet = env
            .fromElements("\n*** " + caption + " ***\n");
    DataSet<String> graphStringSet =
        new CanonicalAdjacencyMatrixBuilder()
            .execute(graph);

    outSet = outSet
        .cross(captionSet)
        .with(new OutputAppender())
        .cross(graphStringSet)
        .with(new OutputAppender());
  }

  public void addSelectedVertices(String caption, DataSet<Vertex<Long, ObjectMap>> vertices,
                                  ArrayList<Long> vertexList) {
    DataSet<Tuple1<Long>> vertexIds = env.fromCollection(vertexList)
        .map((MapFunction<Long, Tuple1<Long>>) Tuple1::new);
    DataSet<String> captionSet = env
        .fromElements("\n*** " + caption + " ***\n");
    DataSet<String> vertexSet = vertices
        .rightOuterJoin(vertexIds)
        .where(0)
        .equalTo(0)
        .with(new FinalVertexOutputJoinFunction())
        .reduceGroup(new ConcatStrings());

    outSet = outSet
        .cross(captionSet)
        .with(new OutputAppender())
        .cross(vertexSet)
        .with(new OutputAppender());
  }

  public void addEdges(String caption, Graph<Long, ObjectMap, ObjectMap> graph, ArrayList<Long> vertexList) {

    DataSet<Long> vertexIds = env.fromCollection(vertexList);

    DataSet<String> captionSet = env
        .fromElements("\n*** " + caption + " ***\n");

    DataSet<Edge<Long, ObjectMap>> edges = graph.getEdges()
        .filter(new RichFilterFunction<Edge<Long, ObjectMap>>() {
          public List<Long> vertexIds;

          @Override
          public void open(Configuration parameters) {
            this.vertexIds = getRuntimeContext().getBroadcastVariable("vertexIds");
          }

          @Override
          public boolean filter(Edge<Long, ObjectMap> edge) throws Exception {
            if (vertexIds.contains(edge.getSource()) || vertexIds.contains(edge.getTarget())) {
              return true;
            } else {
              return false;
            }
          }
        }).withBroadcastSet(vertexIds, "vertexIds");

    DataSet<String> edgeSet = edges
        .map(edge -> {
          String simValue = "";
          if (edge.getValue().containsKey(Constants.AGGREGATED_SIM_VALUE)) {
            simValue = " " + edge.getValue().getEdgeSimilarity().toString();
          }
          return "(" + edge.getSource() + ", " + edge.getTarget() + ")" + simValue;
        })
        .returns(new TypeHint<String>() {})
        .reduceGroup(new ConcatStrings());

    outSet = outSet
        .cross(captionSet)
        .with(new OutputAppender())
        .cross(edgeSet)
        .with(new OutputAppender());
  }

  /**
   * For each chosen basic component id, show the changed final vertices
   *
   * example:
   *     out.addRandomBaseClusters("base random clusters with vertex properties from final",
   * graph.getVertices(),
   * representativeVertices, 20);
   * @param caption output caption
   * @param baseVertices random base vertices
   * @param finalVertices final vertices with properties
   * @param amount amount of clusters
   */
  public void addRandomBaseClusters(String caption, DataSet<Vertex<Long, ObjectMap>> baseVertices,
                                    DataSet<Vertex<Long, ObjectMap>> finalVertices, int amount) {
    DataSet<Tuple1<Long>> randomCcIds = baseVertices
        .map(vertex -> new Tuple1<>((long) vertex.getValue().get(Constants.CC_ID)))
        .returns(new TypeHint<Tuple1<Long>>() {})
        .distinct()
        .first(amount);

    addFinalVertexValues(caption, randomCcIds, finalVertices, baseVertices);
  }

  /**
   * For each chosen final cluster, provide original information for the contained vertices
   * example:
   *      out.addRandomFinalClustersWithMinSize(
   *        "final random clusters with vertex properties from preprocessing",
   *        representativeVertices,
   *        graph.getVertices(),
   *        15, 20);
   * @param caption output caption
   * @param vertices cluster vertices
   * @param basicVertices basic vertices are used to find the original vertex properties
   * @param minClusterSize minimum size of clusters
   * @param amount amount of clusters
   */
  public void addRandomFinalClustersWithMinSize(String caption, DataSet<Vertex<Long, ObjectMap>> vertices,
                                                DataSet<Vertex<Long, ObjectMap>> basicVertices,
                                                int minClusterSize, int amount) {
    DataSet<String> captionSet = env.fromElements("\n*** " + caption + " ***\n");
    DataSet<Vertex<Long, ObjectMap>> randomClusters = vertices
        .filter(new MinClusterSizeFilterFunction(minClusterSize))
        .first(amount);

    DataSet<String> vertexSet = new CanonicalAdjacencyMatrixBuilder()
        .executeOnRandomFinalClusterBaseVertexValues(randomClusters, basicVertices);

    outSet = outSet
        .cross(captionSet)
        .with(new OutputAppender())
        .cross(vertexSet)
        .with(new OutputAppender());
  }

  /**
   * Custom eval method to print results for the small geo dataset.
   */
  public void printEvalThreePercent(String caption,
                                    DataSet<Vertex<Long, ObjectMap>> mergedClusters,
                                    DataSet<Vertex<Long, ObjectMap>> vertices) {
    if (Constants.INPUT_DIR.contains("perfect")) {
      addClusterSampleToOutput(caption + " 2 size cluster", mergedClusters, vertices, 2, 6); // 2
      addClusterSampleToOutput(caption + " 3 size cluster", mergedClusters, vertices, 3, 9); // 8
      addClusterSampleToOutput(caption + " 4 size cluster", mergedClusters, vertices, 4, 64); //72
    }

    if (Constants.INPUT_DIR.contains("linklion")) {
      addClusterSampleToOutput(caption + " 2 size cluster", mergedClusters, vertices, 2, 37);
      addClusterSampleToOutput(caption + " 3 size cluster", mergedClusters, vertices, 3, 36);
      addClusterSampleToOutput(caption + " 4 size cluster", mergedClusters, vertices, 4, 48);
      addClusterSampleToOutput(caption + " 5 size cluster", mergedClusters, vertices, 5, 19);
    }
  }

  /**
   * WIP reload existing files manually.
   */
  private void addClusterSampleToOutput(String caption, DataSet<Vertex<Long, ObjectMap>> mergedClusters,
                                        DataSet<Vertex<Long, ObjectMap>> vertices,
                                        int clusterSize,
                                        int entityCount) {
    DataSet<String> captionSet = env.fromElements("\n*** " + caption + " ***\n");
    String evalFile = caption.replaceAll("\\D+","").concat("-size-clusters");
    LOG.info("###eval file " + evalFile);

//    String vertexPath = Utils.getFinalPath(evalFile, false);
//    LOG.info("###eval vertex path " + vertexPath + " " + (new File(vertexPath).exists()));

    DataSet<Vertex<Long, ObjectMap>> resultClusters;

//    if (new File(vertexPath).exists()) {
//      LOG.info("###eval sample vertices from file " + vertexPath);
//      resultClusters = Utils.readVerticesFromJSONFile(evalFile, env, false);
//    } else {
      LOG.info("###eval vertices from mergedclusters, pick sample");
      resultClusters = mergedClusters
          .filter(new ClusterSizeSimpleFilterFunction(clusterSize))
          .first(entityCount);
      /**
       * Write eval clusters for a certain size to disk for later reuse.
       */
      Utils.writeVerticesToJSONFile(resultClusters, evalFile);
//    }

    DataSet<String> vertexSet = new CanonicalAdjacencyMatrixBuilder()
        .executeOnRandomFinalClusterBaseVertexValues(resultClusters, vertices);

    outSet = outSet
        .cross(captionSet)
        .with(new OutputAppender())
        .cross(vertexSet)
        .with(new OutputAppender());
  }

  /**
   * For a list of base cluster id's, print the base entity properties and the FINAL cluster id.
   */
  public void addSelectedBaseClusters(String caption, DataSet<Vertex<Long, ObjectMap>> baseVertices,
                                                  DataSet<Vertex<Long, ObjectMap>> finalVertices,
                                                  ArrayList<Long> vertexList) {
    DataSet<Tuple1<Long>> vertexListTuple = env.fromCollection(vertexList)
        .map(Tuple1::new) // method references
        .returns(new TypeHint<Tuple1<Long>>() {});

    addFinalVertexValues(caption, vertexListTuple, finalVertices, baseVertices);
  }

  /**
   * Add final vertex component ids for random/selected base vertices.
   */
  private void addFinalVertexValues(String caption, DataSet<Tuple1<Long>> vertexListTuple,
                                    DataSet<Vertex<Long, ObjectMap>> finalVertices,
                                    DataSet<Vertex<Long, ObjectMap>> baseVertices) {
    DataSet<String> captionSet = env.fromElements("\n*** " + caption + " ***\n");

    DataSet<Vertex<Long, ObjectMap>> randomClusters = vertexListTuple
        .leftOuterJoin(baseVertices)
        .where(0)
        .equalTo(new CcIdKeySelector())
        .with(new ExtractSelectedVerticesFlatJoinFunction());

    DataSet<String> vertexSet = new CanonicalAdjacencyMatrixBuilder()
        .executeOnBaseClusters(randomClusters, finalVertices);

    outSet = outSet
        .cross(captionSet)
        .with(new OutputAppender())
        .cross(vertexSet)
        .with(new OutputAppender());
  }

  public void addPreClusterSizes(String caption, DataSet<Vertex<Long, ObjectMap>> vertices,
                                 final String compName) {
    if (Constants.VERBOSITY.equals(Constants.DEBUG) || Constants.VERBOSITY.equals(Constants.INFO)) {
      DataSet<String> captionSet = env
          .fromElements("\n*** " + caption + " ***\n");

      DataSet<String> vertexSet = vertices
          .map(vertex -> {
//            LOG.info("###preClusterVertex: " + vertex.toString());
            return new Tuple2<>((long) vertex.getValue().get(compName), 1L);
          })
          .returns(new TypeHint<Tuple2<Long, Long>>() {})
          .groupBy(0)
          .sum(1)
          .map(new FrequencyMapByFunction(1))
          .groupBy(0)
          .sum(1)
          .reduceGroup(new ConcatTuple2Longs());

      outSet = outSet
          .cross(captionSet)
          .with(new OutputAppender())
          .cross(vertexSet)
          .with(new OutputAppender());
    }
  }

  /**
   * Add the cluster sizes for the given vertex dataset.
   * @param caption caption
   * @param vertices vertices
   */
  public void addClusterSizes(String caption, DataSet<Vertex<Long, ObjectMap>> vertices) {
    if (Constants.VERBOSITY.equals(Constants.DEBUG) || Constants.VERBOSITY.equals(Constants.INFO)) {
      DataSet<String> captionSet = env
          .fromElements("\n*** " + caption + " ***\n");

      DataSet<String> vertexSet = vertices
          .map(vertex -> new Tuple2<>((long) vertex.getValue().getVerticesList().size(), 1L))
          .returns(new TypeHint<Tuple2<Long, Long>>() {})
          .groupBy(0).sum(1)
          .reduceGroup(new ConcatTuple2Longs());

      outSet = outSet
          .cross(captionSet)
          .with(new OutputAppender())
          .cross(vertexSet)
          .with(new OutputAppender());
    }
  }

  public <T> void addDataSetCount(String caption, DataSet<T> data) {
    DataSet<String> captionSet = env
        .fromElements("\n*** " + caption + " ***\n");
    DataSet<String> dataSet = data
        .map(vertex -> new Tuple1<>(1L))
        .returns(new TypeHint<Tuple1<Long>>() {})
        .sum(0)
        .first(1)
        .map(tuple -> "count: " + tuple.f0.toString())
        .returns(new TypeHint<String>() {});

    outSet = outSet
        .cross(captionSet)
        .with(new OutputAppender())
        .cross(dataSet)
        .with(new OutputAppender());
  }

  public <T> void  addVertexAndEdgeSizes(String caption, Graph<Long, ObjectMap, T> graph) {
    if (Constants.VERBOSITY.equals(Constants.DEBUG) || Constants.VERBOSITY.equals(Constants.INFO)) {
      DataSet<String> captionSet = env
          .fromElements("\n*** " + caption + " ***\n");
      DataSet<String> vertexSet = graph.getVertices()
          .map(vertex -> new Tuple2<>(vertex.getId(), 1L))
          .returns(new TypeHint<Tuple2<Long, Long>>() {})
          .sum(1)
          .first(1)
          .map(tuple -> "vertices: " + tuple.f1.toString())
          .returns(new TypeHint<String>() {});

      DataSet<String> edgeSet = graph.getEdgeIds()
          .map(tuple -> new Tuple1<>(1L))
          .returns(new TypeHint<Tuple1<Long>>() {})
          .sum(0)
          .first(1)
          .map(tuple -> "edges: " + tuple.f0.toString())
          .returns(new TypeHint<String>() {});

      outSet = outSet
          .cross(captionSet)
          .with(new OutputAppender())
          .cross(vertexSet)
          .with(new OutputAppender())
          .cross(edgeSet).with(new OutputAppender());
    }
  }

  public <T> void addTuples(String caption, DataSet<T> tuples) {
    DataSet<String> captionSet = env
        .fromElements("\n*** " + caption + " ***\n");
    DataSet<String> tupleSet =
        new CanonicalAdjacencyMatrixBuilder().executeOnTuples(tuples);

    outSet = outSet
        .cross(captionSet)
        .with(new OutputAppender())
        .cross(tupleSet)
        .with(new OutputAppender());
  }

  public void addVertices(String caption, DataSet<Vertex<Long, ObjectMap>> vertices) {
    DataSet<String> captionSet = env
        .fromElements("\n*** " + caption + " ***\n");
    DataSet<String> vertexSet =
        new CanonicalAdjacencyMatrixBuilder().executeOnVertices(vertices);

    outSet = outSet
        .cross(captionSet)
        .with(new OutputAppender())
        .cross(vertexSet)
        .with(new OutputAppender());
  }

  private static class ConcatTuple2Longs implements GroupReduceFunction<Tuple2<Long, Long>, String> {
    @Override
    public void reduce(Iterable<Tuple2<Long, Long>> tuples, Collector<String> collector) throws Exception {
        List<String> vertexSizeList = new ArrayList<>();

        for (Tuple2<Long, Long> tuple : tuples) {
          vertexSizeList.add("size: " + tuple.f0 + " count: " + tuple.f1);
        }

        Collections.sort(vertexSizeList);
        collector.collect(StringUtils.join(vertexSizeList, "\n"));
    }
  }

  private static class FinalVertexOutputJoinFunction
      implements JoinFunction<Vertex<Long, ObjectMap>, Tuple1<Long>, String> {
    @Override
    public String join(Vertex<Long, ObjectMap> vertex, Tuple1<Long> aLong) throws Exception {
      return vertex == null ? "" : vertex.toString();//Utils.toString(vertex);
    }
  }

  /**
   * Flink function to append output data set.
   */
  private class OutputAppender
      implements CrossFunction<ArrayList<String>, String, ArrayList<String>> {
    @Override
    public ArrayList<String> cross(ArrayList<String> out, String line) throws
        Exception {

      out.add(line);

      return out;
    }
  }

  /**
   * Flink function to combine output lines.
   */
  private class LineCombiner implements MapFunction<ArrayList<String>, String> {
    @Override
    public String map(ArrayList<String> lines) throws Exception {

      return StringUtils.join(lines, "\n");
    }
  }

  /**
   * print output
   * @throws Exception
   */
  public void print() throws Exception {
    outSet.map(new LineCombiner())
        .print();
  }
}
