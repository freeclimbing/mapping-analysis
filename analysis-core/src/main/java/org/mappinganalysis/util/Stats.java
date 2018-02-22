package org.mappinganalysis.util;

import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.preprocessing.utils.InternalTypeMapFunction;
import org.mappinganalysis.model.functions.stats.FrequencyMapByFunction;
import org.mappinganalysis.model.functions.stats.SourceSortFunction;

import java.util.List;
import java.util.Map;

/**
 * Helper methods for Flink mapping analysis
 */
public class Stats {
  private static final Logger LOG = Logger.getLogger(Stats.class);
  /**
   * Count resources per component for a given flink connected component result set.
   * @param ccResult Tuple2 with VertexId, ComponentId
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
            return vertex.getValue().getDataSource();
          }
        })
        .reduceGroup(new GroupReduceFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>>() {
          @Override
          public void reduce(Iterable<Vertex<Long, ObjectMap>> iterable, Collector<Vertex<Long, ObjectMap>> collector) throws Exception {
            long count = 0;
            Vertex<Long, ObjectMap> result = new Vertex<>();
            ObjectMap resultProps = new ObjectMap(Constants.GEO);
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

    LOG.info("[3] ### Representatives created: "
        + jobExecResult.getAccumulatorResult(Constants.REPRESENTATIVE_ACCUMULATOR)); // MajorityPropertiesGRFunction
  }

  /**
   * used in a test
   * Get vertex count having correct geo coordinates / type: (geo, type)
   * @param isAbsolutePath default false, needed for test
   */
  public static DataSet<Tuple2<Integer, Integer>> countMissingGeoAndTypeProperties(
      String path, boolean isAbsolutePath, ExecutionEnvironment env) throws Exception {
    Graph<Long, ObjectMap, NullValue> preGraph =
        new JSONDataSource(path, isAbsolutePath, env)
            .getGraph(ObjectMap.class, NullValue.class);

    DataSet<Tuple2<Integer, Integer>> geoTypeTuples = preGraph
        .mapVertices(new InternalTypeMapFunction())
        .getVertices()
        .map(vertex -> {
          int geo = 0;
          if (vertex.getValue().hasGeoPropertiesValid()) {
            geo = 1;
          }
          int type = 0;
          if (vertex.getValue().containsKey(Constants.TYPE_INTERN)
              && !vertex.getValue().hasTypeNoType(Constants.TYPE_INTERN)) {
            type = 1;
          }
          return new Tuple2<>(geo, type);
        })
        .returns(new TypeHint<Tuple2<Integer, Integer>>() {});

    return geoTypeTuples.sum(0).andSum(1);
  }

  /**
   * Count sources per data source.
   */
  public static DataSet<Tuple2<String, Integer>> printVertexSourceCounts(
      Graph<Long, ObjectMap, NullValue> graph) throws Exception {

    return graph.getVertices()
        .map(new MapFunction<Vertex<Long,ObjectMap>, Tuple3<Long, String, Integer>>() {
          @Override
          public Tuple3<Long, String, Integer> map(Vertex<Long, ObjectMap> vertex) throws Exception {
            return new Tuple3<Long, String, Integer>(vertex.getId(), vertex.getValue().getDataSource(), 1);
          }
        })
        .groupBy(1)
        .reduce((left, right) -> new Tuple3<>(left.f0, left.f1, left.f2 + right.f2))
        .map(value -> new Tuple2<>(value.f1, value.f2))
        .returns(new TypeHint<Tuple2<String, Integer>>() {});
  }

  /**
   *  Print counts of edges between different sources. It's already without
   *  looking at the edge direction, so 2 edges (dbp, gn) and (gn, dbp)
   *  get counted as (dbp, gn, 2)
   */
  public static DataSet<Tuple3<String, String, Integer>> printEdgeSourceCounts(
      Graph<Long, ObjectMap, NullValue> graph) throws Exception {
    DataSet<Tuple2<Long, Long>> edges = graph.getEdgeIds()
        .distinct();

    return getEdgeSourceTuples(graph, edges)
        .map(new SourceSortFunction())
        .groupBy(0, 1)
        .reduce((left, right) -> new Tuple3<>(left.f0, left.f1, left.f2 + right.f2));
  }

  /**
   * Return a tuple of edge ids and their corresponding data source string.
   * TODO most likely too much "distinct"
   * @return tuple (1, 2, dbp, gn)
   */
  private static DataSet<Tuple4<Long, Long, String, String>> getEdgeSourceTuples(
      Graph<Long, ObjectMap, NullValue> graph,
      DataSet<Tuple2<Long, Long>> edges) {

    return edges
        .map(edge -> {
          if (edge.f0 < edge.f1) {
            return edge;
          } else {
            return edge.swap();
          }
        })
        .returns(new TypeHint<Tuple2<Long, Long>>() {})
        .distinct()
        .leftOuterJoin(graph.getVertices())
        .where(0)
        .equalTo(0)
        .with(new FlatJoinFunction<Tuple2<Long,Long>,
            Vertex<Long,ObjectMap>,
            Tuple3<Long, Long, String>>() {
          @Override
          public void join(Tuple2<Long, Long> left,
                           Vertex<Long, ObjectMap> right,
                           Collector<Tuple3<Long, Long, String>> out) throws Exception {
            if (left != null) {
//                LOG.info("first1: " + left.toString() + " "  + right.toString());
              out.collect(new Tuple3<>(left.f0, left.f1, right.getValue().getDataSource()));
            }
          }
        })
        .leftOuterJoin(graph.getVertices())
        .where(1)
        .equalTo(0)
        .with(new FlatJoinFunction<Tuple3<Long,Long,String>, Vertex<Long,ObjectMap>, Tuple4<Long, Long, String, String>>() {
          @Override
          public void join(Tuple3<Long, Long, String> left,
                           Vertex<Long, ObjectMap> right,
                           Collector<Tuple4<Long, Long, String, String>> out) throws Exception {
            if (left != null) {
//                LOG.info("second1: " + left.toString() + " "  + right.toString());
              if (left.f2.equals(right.getValue().getDataSource())) {
                LOG.info("###anomaly: " + left + " " + right);
              }
              out.collect(new Tuple4<>(left.f0, left.f1, left.f2, right.getValue().getDataSource()));
            }
          }
        })
        .distinct(0,1);
  }
}
