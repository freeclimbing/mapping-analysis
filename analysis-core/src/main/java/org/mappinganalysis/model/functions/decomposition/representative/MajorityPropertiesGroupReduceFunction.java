package org.mappinganalysis.model.functions.decomposition.representative;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.GeoCode;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.merge.Merge;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.HashMap;
import java.util.Set;

/**
 * Merge properties for representative.
 */
public class MajorityPropertiesGroupReduceFunction
    extends RichGroupReduceFunction<Vertex<Long, ObjectMap>,
    Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(MajorityPropertiesGroupReduceFunction.class);
  private LongCounter representativeCount = new LongCounter();

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(Constants.REPRESENTATIVE_ACCUMULATOR, representativeCount);
  }

  @Override
  public void reduce(Iterable<Vertex<Long, ObjectMap>> vertices,
                     Collector<Vertex<Long, ObjectMap>> collector) throws Exception {
    Vertex<Long, ObjectMap> resultVertex = new Vertex<>(); // don't use reuseVertex here
    ObjectMap resultProps = new ObjectMap();
    Set<Long> clusterVertices = Sets.newHashSet();
    Set<String> clusterOntologies = Sets.newHashSet();
    HashMap<String, Integer> labelMap = Maps.newHashMap();
    HashMap<String, GeoCode> geoMap = Maps.newHashMap();

    for (Vertex<Long, ObjectMap> vertex : vertices) {
      updateVertexId(resultVertex, vertex);
      updateClusterVertexIds(clusterVertices, vertex);
      updateClusterOntologies(clusterOntologies, vertex);

      addLabelToMap(labelMap, vertex);

      resultProps.addTypes(Constants.TYPE_INTERN, vertex.getValue().getTypes(Constants.TYPE_INTERN));
      addGeoToMap(geoMap, vertex);

      if (vertex.getValue().containsKey(Constants.OLD_HASH_CC)) {
        resultProps.put(Constants.OLD_HASH_CC,
            vertex.getValue().get(Constants.OLD_HASH_CC));
      }
    }

    if (!geoMap.isEmpty()) {
      resultProps.setGeoProperties(geoMap);
    }
    if (!labelMap.isEmpty()) {
      resultProps.put(Constants.LABEL, Merge.getFinalValue(labelMap, Constants.LABEL));
    }

    resultProps.setClusterSources(clusterOntologies);
    resultProps.setClusterVertices(clusterVertices);

    resultVertex.setValue(resultProps);
    representativeCount.add(1L);

    collector.collect(resultVertex);
  }

  private void updateClusterOntologies(Set<String> clusterOntologies, Vertex<Long, ObjectMap> currentVertex) {
    if (currentVertex.getValue().containsKey(Constants.ONTOLOGY)) {
      clusterOntologies.add(currentVertex.getValue().getOntology());
    }
    if (currentVertex.getValue().containsKey(Constants.ONTOLOGIES)) {
      clusterOntologies.addAll(currentVertex.getValue().getOntologiesList());
    }
  }

  private void updateClusterVertexIds(Set<Long> clusterVertices, Vertex<Long, ObjectMap> currentVertex) {
    clusterVertices.add(currentVertex.getId());
    if (currentVertex.getValue().containsKey(Constants.CL_VERTICES)) {
      clusterVertices.addAll(currentVertex.getValue().getVerticesList());
    }
  }

  private void updateVertexId(Vertex<Long, ObjectMap> resultVertex, Vertex<Long, ObjectMap> currentVertex) {
    if (resultVertex.getId() == null || currentVertex.getId() < resultVertex.getId()) {
      resultVertex.setId(currentVertex.getId());
    }
  }

  private void addGeoToMap(HashMap<String, GeoCode> geoMap, Vertex<Long, ObjectMap> vertex) {
    if (vertex.getValue().hasGeoPropertiesValid()) {
      if (!vertex.getValue().containsKey(Constants.ONTOLOGY)
          && !vertex.getValue().containsKey(Constants.ONTOLOGIES)) {
        LOG.info("no/more ont but geo: " + vertex);
      }

      Double latitude = vertex.getValue().getLatitude();
      Double longitude = vertex.getValue().getLongitude();

      if (vertex.getValue().containsKey(Constants.ONTOLOGY)) {
        geoMap.put(vertex.getValue().getOntology(),
            new GeoCode(latitude, longitude));
      } else if (vertex.getValue().containsKey(Constants.ONTOLOGIES)) {
        for (String value : vertex.getValue().getOntologiesList()) {
          geoMap.put(value, new GeoCode(latitude, longitude));
        }
      }
    }
  }

  private void addLabelToMap(HashMap<String, Integer> labelMap, Vertex<Long, ObjectMap> currentVertex) {
    if (currentVertex.getValue().containsKey(Constants.LABEL)) {
      String label = Utils.simplify(currentVertex.getValue().getLabel());
      if (labelMap.containsKey(label)) {
        int labelCount = labelMap.get(label);
        labelMap.put(label, labelCount + 1);
      } else {
        labelMap.put(label, 1);
      }
    }
  }
}
