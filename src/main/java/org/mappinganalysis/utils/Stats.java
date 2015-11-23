package org.mappinganalysis.utils;

import com.google.common.collect.Maps;
import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.mappinganalysis.MySQLToFlink;
import org.mappinganalysis.model.FlinkVertex;

import java.util.List;
import java.util.Map;

/**
 * Helper methods for Flink mapping analysis
 */
public class Stats {

  /**
   * Count resources per component for a given flink connected component result set.
   * @param ccResult dataset to be analyzed
   * @throws Exception
   */
  public static void countPrintResourcesPerCc(DataSet<Tuple2<Long, Long>> ccResult) throws Exception {
    List<Tuple2<Long, Long>> ccGeoList = ccResult
        .groupBy(1)
        .reduceGroup(new GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
          @Override
          public void reduce(Iterable<Tuple2<Long, Long>> component, Collector<Tuple2<Long, Long>> out) throws Exception {
            long count = 0;
            long id = 0;
            for (Tuple2<Long, Long> vertex : component) {
              count++;
              id = vertex.f1;
            }
            out.collect(new Tuple2<>(id, count));
          }
        })
        .collect();

    int one = 0;
    int two = 0;
    int three = 0;
    int four = 0;
    int five = 0;
    int six = 0;
    int seven = 0;
    for (Tuple2<Long, Long> tuple2 : ccGeoList) {
      if (tuple2.f1 == 1) {
        one++;
      } else if (tuple2.f1 == 2) {
        two++;
      } else if (tuple2.f1 == 3) {
        three++;
      } else if (tuple2.f1 == 4) {
        four++;
      } else if (tuple2.f1 == 5) {
        five++;
      } else if (tuple2.f1 == 6) {
        six++;
      } else if (tuple2.f1 == 7) {
        seven++;
      }
    }
    System.out.println("one: " + one + " two: " + two + " three: " + three +
        " four: " + four + " five: " + five + " six: " + six + " seven: " + seven);
  }

  // duplicate methods in emptygeocodefilter
  public static void countPrintGeoPointsPerOntology() throws Exception {
    Graph<Long, FlinkVertex, NullValue> tgraph = MySQLToFlink.getInputGraph();
    tgraph.getVertices()
        .filter(new FilterFunction<Vertex<Long, FlinkVertex>>() {
          @Override
          public boolean filter(Vertex<Long, FlinkVertex> vertex) throws Exception {
            Map<String, Object> props = vertex.getValue().getProperties();
            if (props.containsKey("lat") && props.containsKey("lon")) {
              Object lat = props.get("lat");
              Object lon = props.get("lon");
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
        .groupBy(new KeySelector<Vertex<Long, FlinkVertex>, String>() {
          @Override
          public String getKey(Vertex<Long, FlinkVertex> vertex) throws Exception {
            return (String) vertex.getValue().getProperties().get(Utils.ONTOLOGY);
          }
        })
        .reduceGroup(new GroupReduceFunction<Vertex<Long, FlinkVertex>, Vertex<Long, FlinkVertex>>() {
          @Override
          public void reduce(Iterable<Vertex<Long, FlinkVertex>> iterable, Collector<Vertex<Long, FlinkVertex>> collector) throws Exception {
            long count = 0;
            FlinkVertex result = new FlinkVertex();
            Map<String, Object> resultProps = Maps.newHashMap();
            boolean isVertexPrepared = false;

            for (Vertex<Long, FlinkVertex> vertex : iterable) {
              count++;
              if (!isVertexPrepared) {
                resultProps = vertex.getValue().getProperties();
                result.setId(vertex.getId());
                isVertexPrepared = true;
              }
            }
            resultProps.put("count", count);
            result.setProperties(resultProps);
            collector.collect(new Vertex<>(result.getId(), result));
          }
        })
        .print();
  }

}
