package org.mappinganalysis.utils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.MappingAnalysisExample;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.stats.ResultComponentSelectionFilter;
import org.mappinganalysis.model.functions.stats.ResultEdgesSelectionFilter;
import org.mappinganalysis.model.functions.stats.ResultVerticesSelectionFilter;

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

  public static void writeVerticesToLog(Graph<Long, ObjectMap, ObjectMap> graph,
                                         List<Long> clusterList) throws Exception {
    graph.filterOnVertices(new ResultVerticesSelectionFilter(clusterList))
        .getVertices().collect();
  }

  public static void writeCcToLog(Graph<Long, ObjectMap, ObjectMap> graph,
                                   List<Long> clusterList, String ccType) throws Exception {
    graph.filterOnVertices(new ResultComponentSelectionFilter(clusterList, ccType))
        .getVertices().collect();
  }

  public static void printLabelsForMergedClusters(DataSet<Vertex<Long, ObjectMap>> clusters)
      throws Exception {
    for (Vertex<Long, ObjectMap> vertex : clusters.collect()) {
      Map<String, Object> properties = vertex.getValue();
      Object clusteredVerts = properties.get(Utils.CL_VERTICES);

      if (vertex.getValue().get(Utils.TYPE_INTERN) != null &&
          vertex.getValue().get(Utils.TYPE_INTERN).equals("Settlement")) {
        continue;
      }

      if (clusteredVerts instanceof Set) {
        if (((Set) clusteredVerts).size() < 4) {
          continue;
        }
        LOG.info("---------------------------");
        LOG.info(vertex.toString() + "\n");
        Set<Vertex<Long, ObjectMap>> vertices = Sets.newHashSet((Set<Vertex<Long, ObjectMap>>) clusteredVerts);

        for (Vertex<Long, ObjectMap> clVertex : vertices) {
          LOG.info(clVertex.getValue().get(Utils.LABEL) + "### " + clVertex.getValue().get(Utils.TYPE_INTERN));
        }
      }
      else {
        LOG.info("---------------------------");
        LOG.info(vertex.toString() + "\n");

        Vertex<Long, ObjectMap> tmp = (Vertex<Long, ObjectMap>) clusteredVerts;
        LOG.info(tmp.getValue().get(Utils.LABEL) + "### " + tmp.getValue().get(Utils.TYPE_INTERN) + "\n");
      }
    }
  }

  /**
   * Count resources per component for a given flink connected component result set.
   * @param ccResult dataset to be analyzed
   * @throws Exception
   */
  public static void countPrintResourcesPerCc(DataSet<Tuple2<Long, Long>> ccResult) throws Exception {
    DataSet<Tuple2<Long, Long>> tmpResult = ccResult
        .map(new FrequencyMapFunction())
        .groupBy(1)
        .sum(2)
        .project(1, 2);
    DataSet<Tuple3<Long, Long, Long>> result = tmpResult
        .map(new FrequencyMapFunction()).groupBy(1).sum(2);

    for (Tuple3<Long, Long, Long> tuple : result.collect()) {
      LOG.info("Component size: " + tuple.f1 + ": " + tuple.f2 +  " " + tuple.f0);
    }
  }

  // duplicate methods in emptygeocodefilter
  public static void countPrintGeoPointsPerOntology(Graph<Long, ObjectMap, NullValue> preprocGraph) throws Exception {
    preprocGraph.getVertices()
        .filter(new FilterFunction<Vertex<Long, ObjectMap>>() {
          @Override
          public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
            Map<String, Object> props = vertex.getValue();
            if (props.containsKey(Utils.LAT) && props.containsKey(Utils.LON)) {
              Object lat = props.get(Utils.LAT);
              Object lon = props.get(Utils.LON);
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
            return (String) vertex.getValue().get(Utils.ONTOLOGY);
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

  public static void printAccumulatorValues(ExecutionEnvironment env, DataSet<Tuple2<Long, Long>> edgeIds)
      throws Exception {
    //vertices needs to be computed already
    JobExecutionResult jobExecResult = env.getLastJobExecutionResult();
    LOG.info("[0] Statistics: Get Data");
    LOG.info("[0] Vertices imported: " + jobExecResult.getAccumulatorResult(Utils.VERTEX_COUNT_ACCUMULATOR));
    LOG.info("[0] Edges imported: " + jobExecResult.getAccumulatorResult(Utils.EDGE_COUNT_ACCUMULATOR));
    LOG.info("[0] Properties imported: " + jobExecResult.getAccumulatorResult(Utils.PROP_COUNT_ACCUMULATOR));

    edgeIds.collect(); // how to get rid of this collect job TODO
    LOG.info("[1] Statistics: Preprocessing");
    Map<String, Long> typeStats = Maps.newHashMap();
    List<String> typesList = jobExecResult.getAccumulatorResult(Utils.TYPES_COUNT_ACCUMULATOR);
    for (String s : typesList) {
      if (typeStats.containsKey(s)) {
        typeStats.put(s, typeStats.get(s) + 1L);
      } else {
        typeStats.put(s, 1L);
      }
    }

    LOG.info("[1] Types parsed to internal type: ");
    for (Map.Entry<String, Long> entry : typeStats.entrySet()) {
      LOG.info("[1] " + entry.getKey() + ": " + entry.getValue());
    }

    if (Utils.IS_LINK_FILTER_ACTIVE) {
      LOG.info("[1] Links filtered (strategy: delete 1:n links): "
          + jobExecResult.getAccumulatorResult(Utils.LINK_FILTER_ACCUMULATOR));
      List<Edge<Long, NullValue>> filteredLinksList
          = jobExecResult.getAccumulatorResult(Utils.FILTERED_LINKS_ACCUMULATOR);
      for (Edge<Long, NullValue> edge : filteredLinksList) {
        LOG.info("[1] Link filtered: (" + edge.getSource() + ", " + edge.getTarget() + ")");
      }
    }

    LOG.info("[3] Clustering: Compute all edges within clusters: "
        + jobExecResult.getAccumulatorResult(Utils.RESTRICT_EDGE_COUNT_ACCUMULATOR));
  }

  /**
   * optional stats method
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

  private static class FrequencyMapFunction implements MapFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Long>> {
    @Override
    public Tuple3<Long, Long, Long> map(Tuple2<Long, Long> tuple) throws Exception {
      return new Tuple3<>(tuple.f0, tuple.f1, 1L);
    }
  }
}
