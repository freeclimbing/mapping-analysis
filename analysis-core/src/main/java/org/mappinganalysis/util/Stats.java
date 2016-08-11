package org.mappinganalysis.util;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.output.ExampleOutput;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.Preprocessing;
import org.mappinganalysis.model.functions.stats.FrequencyMapByFunction;
import org.mappinganalysis.model.functions.stats.ResultComponentSelectionFilter;
import org.mappinganalysis.model.functions.stats.ResultEdgesSelectionFilter;
import org.mappinganalysis.model.functions.stats.ResultVerticesSelectionFilter;
import org.mappinganalysis.util.functions.filter.ClusterSizeSimpleFilterFunction;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Helper methods for Flink mapping analysis
 */
public class Stats {
  private static final Logger LOG = Logger.getLogger(Stats.class);

  public static void writeEdgesToLog(Graph<Long, ObjectMap, ObjectMap> oneIterationGraph,
                                      List<Long> clusterStats) throws Exception {
    oneIterationGraph.filterOnEdges(new ResultEdgesSelectionFilter(clusterStats))
        .getEdges().collect();
  }

  public static void writeVerticesToLog(DataSet<Vertex<Long, ObjectMap>> vertices,
                                         List<Long> clusterList) throws Exception {
    vertices.filter(new ResultVerticesSelectionFilter(clusterList)).collect();
  }

  public static void writeCcToLog(DataSet<Vertex<Long, ObjectMap>> vertices,
                                   List<Long> clusterList, String ccType) throws Exception {
    DataSet<Vertex<Long, ObjectMap>> filteredVertices = vertices
        .filter(new ResultComponentSelectionFilter(clusterList, ccType))
        .map(new MapFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>>() {
          @Override
          public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {
            vertex.getValue().remove(Constants.TYPE);
            vertex.getValue().remove(Constants.TMP_TYPE);
            vertex.getValue().remove(Constants.DB_URL_FIELD);
            vertex.getValue().remove(Constants.VERTEX_OPTIONS);
            return null;
          }
        });

    for (Vertex<Long, ObjectMap> vertex : filteredVertices.collect()) {
      LOG.info(vertex);
    }

  }

  /**
   * [deprecated]?
   * Count resources per component for a given flink connected component result set.
   * @param ccResult Tuple2 with VertexId, ComponentId
   * @throws Exception
   */
  public static void countPrintResourcesPerCc(DataSet<Tuple2<Long, Long>> ccResult) throws Exception {
    DataSet<Tuple2<Long, Long>> tmpResult = ccResult
        .map(new FrequencyMapByFunction(1)) // VertexId, ComponentId, 1L
        .groupBy(0)
        .sum(1); // ComponentId, Sum(1L)
    DataSet<Tuple2<Long, Long>> result = tmpResult
        .map(new FrequencyMapByFunction(1)).groupBy(0).sum(1);

    for (Tuple2<Long, Long> tuple : result.collect()) {
      LOG.info("Component size: " + tuple.f1 + ": " + tuple.f0);
    }
  }

  // duplicate methods in emptygeocodefilter
  public static void countPrintGeoPointsPerOntology(Graph<Long, ObjectMap, NullValue> preprocGraph) throws Exception {
    preprocGraph.getVertices()
        .filter(new FilterFunction<Vertex<Long, ObjectMap>>() {
          @Override
          public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
            Map<String, Object> props = vertex.getValue();
            if (props.containsKey(Constants.LAT) && props.containsKey(Constants.LON)) {
              Object lat = props.get(Constants.LAT);
              Object lon = props.get(Constants.LON);
              return ((getDouble(lat) == null) || (getDouble(lon) == null)) ? Boolean.FALSE : Boolean.TRUE;
            } else {
              return Boolean.FALSE;
            }
          }

          private Double getDouble(Object latlon) {
            if (latlon instanceof List) {
              return Doubles.tryParse(((List) latlon).get(0).toString());
            } else {
              return Doubles.tryParse(latlon.toString());
            }
          }
        })
        .groupBy(new KeySelector<Vertex<Long, ObjectMap>, String>() {
          @Override
          public String getKey(Vertex<Long, ObjectMap> vertex) throws Exception {
            return vertex.getValue().getOntology();
          }
        })
        .reduceGroup(new GroupReduceFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>>() {
          @Override
          public void reduce(Iterable<Vertex<Long, ObjectMap>> iterable, Collector<Vertex<Long, ObjectMap>> collector) throws Exception {
            long count = 0;
            Vertex<Long, ObjectMap> result = new Vertex<>();
            ObjectMap resultProps = new ObjectMap();
            boolean isVertexPrepared = false;

            for (Vertex<Long, ObjectMap> vertex : iterable) {
              count++;
              if (!isVertexPrepared) {
                resultProps = vertex.getValue();
                result.setId(vertex.getId());
                isVertexPrepared = true;
              }
            }
            resultProps.put("count", count);
            result.setValue(resultProps);
            collector.collect(new Vertex<>(result.getId(), result.getValue()));
          }
        })
        .print();
  }

  /**
   * Accumulator values for an ongoing flink workflow. Not working if graph is reloaded from JSON.
   * @throws Exception
   */
  public static <T> void printAccumulatorValues(ExecutionEnvironment env, Graph<Long, ObjectMap, T> graph)
      throws Exception {

    JobExecutionResult jobExecResult = env.getLastJobExecutionResult();
    if (jobExecResult == null) {
      graph.getVertexIds().collect();
      jobExecResult = env.getLastJobExecutionResult();
    }

    LOG.info("[1] ### BaseVertexCreator vertex counter: "
        + jobExecResult.getAccumulatorResult(Constants.BASE_VERTEX_COUNT_ACCUMULATOR));
    LOG.info("[1] ### PropertyCoGroupFunction vertex counter: "
        + jobExecResult.getAccumulatorResult(Constants.VERTEX_COUNT_ACCUMULATOR));
    LOG.info("[1] ### FlinkEdgeCreator edge counter: "
        + jobExecResult.getAccumulatorResult(Constants.EDGE_COUNT_ACCUMULATOR));
    LOG.info("[1] ### FlinkPropertyMapper property counter: "
        + jobExecResult.getAccumulatorResult(Constants.PROP_COUNT_ACCUMULATOR));
    LOG.info("[1] ### typeMismatchCorrection wrong edges counter: "
        + jobExecResult.getAccumulatorResult(Constants.EDGE_EXCLUDE_ACCUMULATOR));
    LOG.info("[1] ### applyLinkFilterStrategy correct edges counter: "
        + jobExecResult.getAccumulatorResult(Constants.PREPROC_LINK_FILTER_ACCUMULATOR));

    LOG.info("[3] ### Representatives created: "
        + jobExecResult.getAccumulatorResult(Constants.REPRESENTATIVE_ACCUMULATOR)); // MajorityPropertiesGRFunction
    LOG.info("[3] ### Clusters created in refinement step: "
        + jobExecResult.getAccumulatorResult(Constants.REFINEMENT_MERGE_ACCUMULATOR)); // SimilarClusterMergeMapFunction
    LOG.info("[3] ### Excluded vertex counter: "
        + jobExecResult.getAccumulatorResult(Constants.EXCLUDE_VERTEX_ACCUMULATOR)); // RightSideOnlyJoinFunction


    // todo this working?
    LOG.info("[3] ### Clustering: Compute all edges within clusters: "
        + jobExecResult.getAccumulatorResult(Constants.RESTRICT_EDGE_COUNT_ACCUMULATOR));
    LOG.info("[3] ### Exclude vertices from their component and create new component: "
        + jobExecResult.getAccumulatorResult(Constants.SIMSORT_EXCLUDE_FROM_COMPONENT_ACCUMULATOR));

    // optional, currently not used
//    if (jobExecResult.getAccumulatorResult(Constants.TYPES_COUNT_ACCUMULATOR) != null) {
//      Map<String, Long> typeStats = Maps.newHashMap();
//      List<String> typesList = jobExecResult.getAccumulatorResult(Constants.TYPES_COUNT_ACCUMULATOR);
//      for (String s : typesList) {
//        if (typeStats.containsKey(s)) {
//          typeStats.put(s, typeStats.get(s) + 1L);
//        } else {
//          typeStats.put(s, 1L);
//        }
//      }
//
//      LOG.info("[1] ### Types parsed to internal type: ");
//      for (Map.Entry<String, Long> entry : typeStats.entrySet()) {
//        LOG.info("[1] " + entry.getKey() + ": " + entry.getValue());
//      }
//    }

    if (jobExecResult.getAccumulatorResult(Constants.FILTERED_LINKS_ACCUMULATOR) != null) {
      List<Edge<Long, NullValue>> filteredLinksList
          = jobExecResult.getAccumulatorResult(Constants.FILTERED_LINKS_ACCUMULATOR);
      for (Edge<Long, NullValue> edge : filteredLinksList) {
        LOG.info("[1] Link filtered: (" + edge.getSource() + ", " + edge.getTarget() + ")");
      }
    }
  }

  public static void printResultEdgeCounts(Graph<Long, ObjectMap, NullValue> inputGraph,
                                           ExampleOutput out,
                                           DataSet<Vertex<Long, ObjectMap>> mergedClusters) {
    DataSet<Tuple2<Long, Long>> allResultEdgeIds = mergedClusters
        .flatMap(new FlatMapFunction<Vertex<Long,ObjectMap>, Tuple2<Long, Long>>() {
          @Override
          public void flatMap(Vertex<Long, ObjectMap> vertex, Collector<Tuple2<Long, Long>> collector) throws Exception {
            Set<Long> leftList = Sets.newHashSet(vertex.getValue().getVerticesList());
            Set<Long> rightList = Sets.newHashSet(leftList);
            for (Long left : leftList) {
              rightList.remove(left);
              for (Long right : rightList) {
                if (left < right) {
                  collector.collect(new Tuple2<>(left, right));
                } else {
                  collector.collect(new Tuple2<>(right, left));
                }
              }
            }
          }
        });

    out.addDataSetCount("all result edges count", allResultEdgeIds);

    DataSet<Tuple2<Long, Long>> inputEdgeIds = inputGraph
        .getEdgeIds()
        .map(new MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
          @Override
          public Tuple2<Long, Long> map(Tuple2<Long, Long> tuple) throws Exception {
            if (tuple.f0 < tuple.f1) {
              return tuple;
            } else {
              return new Tuple2<>(tuple.f1, tuple.f0);
            }
          }
        });

    DataSet<Tuple2<Integer, Integer>> newPlusDeletedEdges = allResultEdgeIds.fullOuterJoin(inputEdgeIds)
        .where(0, 1)
        .equalTo(0, 1)
        .with(new FlatJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Integer, Integer>>() {
          @Override
          public void join(Tuple2<Long, Long> left, Tuple2<Long, Long> right,
                           Collector<Tuple2<Integer, Integer>> collector) throws Exception {
            if (left == null) {
              collector.collect(new Tuple2<>(0, 1));
            }
            if (right == null) {
              collector.collect(new Tuple2<>(1, 0));
            }
          }
        }).sum(0).andSum(1);

    out.addTuples("new edges and deleted edges", newPlusDeletedEdges);
  }

  /**
   * only working for small geo dataset
   */
  private static void addCountsForSingleSource(Graph<Long, ObjectMap, NullValue> inputGraph,
                                               ExampleOutput out, final String source) {
    DataSet<Vertex<Long, ObjectMap>> nytVertices = inputGraph.getVertices()
        .filter(vertex -> vertex.getValue().getOntology().equals(Constants.NYT_NS));

    DataSet<Vertex<Long, ObjectMap>> gnVertices = inputGraph.getVertices()
        .filter(vertex -> vertex.getValue().getOntology().equals(source));

    DataSet<Edge<Long, NullValue>> edgeDataSet = Preprocessing
        .deleteEdgesWithoutSourceOrTarget(inputGraph.getEdges(), nytVertices.union(gnVertices));

    out.addDataSetCount("vertex size for " + source, gnVertices);
    out.addDataSetCount("edge size for " + source, edgeDataSet);
  }

  public static void addChangedWhileMergingVertices(ExampleOutput out, DataSet<Vertex<Long, ObjectMap>> representativeVertices, DataSet<Vertex<Long, ObjectMap>> mergedClusters) {
    DataSet<Vertex<Long, ObjectMap>> changedWhileMerging = representativeVertices
        .filter(new ClusterSizeSimpleFilterFunction(4))
        .rightOuterJoin(mergedClusters.filter(new ClusterSizeSimpleFilterFunction(4)))
        .where(0)
        .equalTo(0)
        .with(new FlatJoinFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>,
            Vertex<Long, ObjectMap>>() {
          @Override
          public void join(Vertex<Long, ObjectMap> left, Vertex<Long, ObjectMap> right,
                           Collector<Vertex<Long, ObjectMap>> collector) throws Exception {
            if (left == null) {
              collector.collect(right);
            }
          }
        });

//    out.addRandomBaseClusters("random base clusters", preprocGraph.getVertices(), changedWhileMerging, 10);

    out.addVertices("changedWhileMerging", changedWhileMerging);
  }


  /**
   * optional stats method TODO remove collect call
   */
  public static void printEdgesSimValueBelowThreshold(Graph<Long, ObjectMap, NullValue> allGraph,
                                                       DataSet<Triplet<Long, ObjectMap, ObjectMap>>
                                                           accumulatedSimValues) throws Exception {
    LOG.info("accum sim values: " + accumulatedSimValues.count());

    DataSet<Edge<Long, NullValue>> edgesNoSimValue = allGraph.getEdges()
        .leftOuterJoin(accumulatedSimValues)
        .where(0, 1).equalTo(0, 1)
        .with(new FlatJoinFunction<Edge<Long, NullValue>, Triplet<Long, ObjectMap, ObjectMap>,
            Edge<Long, NullValue>>() {
          @Override
          public void join(Edge<Long, NullValue> edge, Triplet<Long, ObjectMap, ObjectMap> triplet,
                           Collector<Edge<Long, NullValue>> collector) throws Exception {
            if (triplet == null) {
              collector.collect(edge);
            }
          }
        });

    // print vertex information for start and target vertex
    edgesNoSimValue
        .leftOuterJoin(allGraph.getVertices())
        .where(0).equalTo(0)
        .with(new JoinFunction<Edge<Long, NullValue>, Vertex<Long, ObjectMap>,
            Triplet<Long, ObjectMap, NullValue>>() {
          @Override
          public Triplet<Long, ObjectMap, NullValue> join(Edge<Long, NullValue> edge,
                                                          Vertex<Long, ObjectMap> vertex) throws Exception {
            return new Triplet<>(edge.getSource(), edge.getTarget(), vertex.getValue(), new ObjectMap(),
                NullValue.getInstance());
          }
        })
        .leftOuterJoin(allGraph.getVertices())
        .where(1).equalTo(0)
        .with(new JoinFunction<Triplet<Long, ObjectMap, NullValue>, Vertex<Long, ObjectMap>, Triplet<Long, ObjectMap,
            NullValue>>() {
          @Override
          public Triplet<Long, ObjectMap, NullValue> join(Triplet<Long, ObjectMap, NullValue> triplet,
                                                          Vertex<Long, ObjectMap> vertex) throws Exception {
            triplet.f3 = vertex.getValue();
            return triplet;
          }
        })
        .print();
  }
}
