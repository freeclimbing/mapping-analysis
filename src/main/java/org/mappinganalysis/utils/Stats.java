package org.mappinganalysis.utils;

import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.MappingAnalysisExample;
import org.mappinganalysis.model.ObjectMap;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Helper methods for Flink mapping analysis
 */
public class Stats {
  private static final Logger LOG = Logger.getLogger(Stats.class);


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
      LOG.info("Component size: " + tuple.f1 + ": " + tuple.f2);
    }
  }

  // duplicate methods in emptygeocodefilter
  public static void countPrintGeoPointsPerOntology() throws Exception {
    Graph<Long, ObjectMap, NullValue> tgraph = MappingAnalysisExample.getInputGraph(Utils.GEO_FULL_NAME);
    tgraph.getVertices()
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

  public static void printAccumulatorValues(ExecutionEnvironment env) throws Exception {
    //vertices needs to be computed already
    JobExecutionResult jobExecResult = env.getLastJobExecutionResult();
    LOG.info("Edges imported: " + jobExecResult.getAccumulatorResult(Utils.EDGE_COUNT_ACCUMULATOR));
    LOG.info("Properties imported: " + jobExecResult.getAccumulatorResult(Utils.PROP_COUNT_ACCUMULATOR));
    LOG.info("Vertices imported: " + jobExecResult.getAccumulatorResult(Utils.VERTEX_COUNT_ACCUMULATOR));

    LOG.info("TMP all edges count: " + jobExecResult.getAccumulatorResult(Utils.TMP_ALL_EDGES_COUNT_ACCUMULATOR));
    LOG.info("Restricted all edges count: " + jobExecResult.getAccumulatorResult(Utils.RESTRICT_EDGE_COUNT_ACCUMULATOR));

//    Map<String, Long> typeStats = Maps.newHashMap();
//    List<String> typesList = vertexJobExecResult.getAccumulatorResult(Utils.TYPES_COUNT_ACCUMULATOR);
//    for (String s : typesList) {
//      if (typeStats.containsKey(s)) {
//        typeStats.put(s, typeStats.get(s) + 1L);
//      } else {
//        typeStats.put(s, 1L);
//      }
//    }
//    LOG.info("### --- Type counts parsed to internal type in preprocessing:");
//    for (Map.Entry<String, Long> entry : typeStats.entrySet()) {
//      LOG.info(entry.getKey() + ": " + entry.getValue());
//    }
//    LOG.info("### type count end");

//    graph.getEdgeIds().collect(); // how to get rid of this collect job TODO
    LOG.info("Number of incorrect links: "
        + jobExecResult.getAccumulatorResult(Utils.LINK_FILTER_ACCUMULATOR));
  }

  private static class FrequencyMapFunction implements MapFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Long>> {
    @Override
    public Tuple3<Long, Long, Long> map(Tuple2<Long, Long> tuple) throws Exception {
      return new Tuple3<>(tuple.f0, tuple.f1, 1L);
    }
  }
}
