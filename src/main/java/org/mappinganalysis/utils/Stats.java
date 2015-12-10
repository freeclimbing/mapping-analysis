package org.mappinganalysis.utils;

import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.mappinganalysis.MappingAnalysisExample;
import org.mappinganalysis.model.ObjectMap;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Helper methods for Flink mapping analysis
 */
public class Stats {

  public static void printLabelsForMergedClusters(DataSet<Vertex<Long, ObjectMap>> clusters)
      throws Exception {
    for (Vertex<Long, ObjectMap> vertex : clusters.collect()) {
      Map<String, Object> properties = vertex.getValue();
      Object clusteredVerts = properties.get(Utils.CL_VERTICES);

      if (clusteredVerts instanceof Set) {
        if (((Set) clusteredVerts).size() < 4) {
          continue;
        }
        System.out.println(vertex.getValue().toString());

        Set<Vertex<Long, ObjectMap>> values = Sets.newHashSet((Set<Vertex<Long, ObjectMap>>) properties.get(Utils.CL_VERTICES));

        for (Vertex<Long, ObjectMap> value : values) {
          System.out.println(value.getValue().get(Utils.LABEL));
        }
      }
      else {
        System.out.println(vertex.getValue().toString());

        Vertex<Long, ObjectMap> tmp = (Vertex<Long, ObjectMap>) clusteredVerts;
        System.out.println(tmp.getValue().get(Utils.LABEL));
      }
    }
  }

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
        })//.print();
        .collect();

    int one = 0;
    int two = 0;
    int three = 0;
    int four = 0;
    int five = 0;
    int six = 0;
    int seven = 0;
    int other = 0;
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
      } else if (tuple2.f1 > 7) {
        other++;
      }
    }
    System.out.println("one: " + one + " two: " + two + " three: " + three +
        " four: " + four + " five: " + five + " six: " + six + " seven: " + seven + " more: " + other);
  }

  // duplicate methods in emptygeocodefilter
  public static void countPrintGeoPointsPerOntology(ExecutionEnvironment environment) throws Exception {
    Graph<Long, ObjectMap, NullValue> tgraph = MappingAnalysisExample.getInputGraph(Utils.GEO_FULL_NAME, environment);
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

}
