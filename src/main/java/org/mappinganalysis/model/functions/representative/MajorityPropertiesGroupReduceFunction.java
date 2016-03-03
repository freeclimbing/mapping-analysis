package org.mappinganalysis.model.functions.representative;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Merge properties of grouped entities based on "best data source" availability,
 * i.e., GeoNames > DBpedia > others //Todo
 */
public class MajorityPropertiesGroupReduceFunction extends RichGroupReduceFunction<Vertex<Long, ObjectMap>,
    Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(MajorityPropertiesGroupReduceFunction.class);
  private LongCounter representativeCount = new LongCounter();

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(Utils.REPRESENTATIVE_ACCUMULATOR, representativeCount);
  }

  @Override
  public void reduce(Iterable<Vertex<Long, ObjectMap>> vertices,
                     Collector<Vertex<Long, ObjectMap>> collector) throws Exception {
    Vertex<Long, ObjectMap> resultVertex = new Vertex<>();
//    LOG.info("result vert without values: " + resultVertex.toString());
    ObjectMap resultProps = new ObjectMap();
    Set<Long> clusterVertices = Sets.newHashSet();
    Set<String> clusterOntologies = Sets.newHashSet();
    HashMap<String, Integer> labelMap = Maps.newHashMap();
    HashMap<String, Integer> typeMap = Maps.newHashMap();

    for (Vertex<Long, ObjectMap> currentVertex : vertices) {
      if (resultVertex.getId() == null || currentVertex.getId() < resultVertex.getId()) {
        resultVertex.setId(currentVertex.getId());
      }
      clusterVertices.add(currentVertex.getId());

      //LABEL
      if (currentVertex.getValue().containsKey(Utils.LABEL)) {
        String label = Utils.simplify(currentVertex.getValue().get(Utils.LABEL).toString());
        if (labelMap.containsKey(label)) {
          int labelCount = labelMap.get(label);
          labelMap.put(label, labelCount + 1);
        } else {
          labelMap.put(label, 1);
        }
      }

      // TYPE
      if (currentVertex.getValue().containsKey(Utils.TYPE_INTERN) && !currentVertex.getValue().hasNoType(Utils.TYPE_INTERN)) {
        String type = currentVertex.getValue().get(Utils.TYPE_INTERN).toString();
        if (typeMap.containsKey(type)) {
          int labelCount = typeMap.get(type);
          typeMap.put(type, labelCount + 1);
        } else {
          typeMap.put(type, 1);
        }
      }

      // GEO
      resultProps.setGeoProperties(currentVertex.getValue());

      if (currentVertex.getValue().containsKey(Utils.ONTOLOGY)) {
        clusterOntologies.add(currentVertex.getValue().get(Utils.ONTOLOGY).toString());
      }
      if (currentVertex.getValue().containsKey(Utils.ONTOLOGIES)) {
        clusterOntologies.addAll((Set<String>) currentVertex.getValue().get(Utils.ONTOLOGIES));
      }

        // OLD HASH
      if (currentVertex.getValue().containsKey(Utils.OLD_HASH_CC)) {
        resultProps.put(Utils.OLD_HASH_CC, currentVertex.getValue().get(Utils.OLD_HASH_CC));
      }
    }

    if (!labelMap.isEmpty()) {
      resultProps.put(Utils.LABEL, getFinalValue(labelMap));
    }
    if (!typeMap.isEmpty()) {
      resultProps.put(Utils.TYPE_INTERN, getFinalValue(typeMap));
    }
    resultProps.put(Utils.ONTOLOGIES, clusterOntologies);
    resultProps.put(Utils.CL_VERTICES, clusterVertices);

    resultVertex.setValue(resultProps);
    representativeCount.add(1L);

    collector.collect(resultVertex);
  }

  private String getFinalValue(HashMap<String, Integer> map) {
    Map.Entry<String, Integer> finalEntry = null;
    for (Map.Entry<String, Integer> entry : map.entrySet()) {
      if (finalEntry == null || Ints.compare(entry.getValue(), finalEntry.getValue()) > 0) {
        finalEntry = entry;
      }
    }

    checkArgument( finalEntry != null, "Entry must not be null" );
    return finalEntry.getKey();
  }
}
