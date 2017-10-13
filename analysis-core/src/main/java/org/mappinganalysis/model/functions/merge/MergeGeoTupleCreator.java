package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

/**
 * Create basic merge tuples for merge process, properties are transferred from
 * Gelly vertices.
 *
 * Care: Initial MergeTuples have some values set to avoid null pointer exceptions.
 * - therefore, dont use reuse tuples here
 */
public class MergeGeoTupleCreator
    implements MapFunction<Vertex<Long, ObjectMap>, MergeGeoTuple> {
  private static final Logger LOG = Logger.getLogger(MergeGeoTupleCreator.class);

  @Override
  public MergeGeoTuple map(Vertex<Long, ObjectMap> vertex) throws Exception {
    MergeGeoTuple tuple = new MergeGeoTuple();

    ObjectMap properties = vertex.getValue();
    properties.setMode(Constants.GEO);

//    LOG.info("PROPERTIES: " + properties.toString() + " " + vertex.getId());
    tuple.setId(vertex.getId());
    tuple.setLabel(properties.getLabel());
    if (properties.hasGeoPropertiesValid()) {
      tuple.setLatitude(properties.getLatitude());
      tuple.setLongitude(properties.getLongitude());
    }
    tuple.setIntTypes(properties.getIntTypes());
    tuple.setIntSources(properties.getIntDataSources());
    if (properties.getVerticesList() == null) {
      properties.setClusterVertices(Sets.newHashSet(vertex.getId()));
    }
    tuple.addClusteredElements(properties.getVerticesList());
    tuple.setBlockingLabel(Utils.getGeoBlockingLabel(properties.getLabel()));

//    LOG.info("### CREATE: " + tuple.toString());
    return tuple;
  }

}