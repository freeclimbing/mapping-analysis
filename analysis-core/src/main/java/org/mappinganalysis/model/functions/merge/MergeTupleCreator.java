package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Utils;

/**
 * Create basic merge tuples for merge process, properties are transferred from
 * Gelly vertices.
 *
 * Care: Initial MergeTuples have some values set to avoid null pointer exceptions.
 * - therefore, dont use reuse tuples here
 */
class MergeTupleCreator implements MapFunction<Vertex<Long, ObjectMap>, MergeTuple> {
  private static final Logger LOG = Logger.getLogger(MergeTupleCreator.class);

  @Override
  public MergeTuple map(Vertex<Long, ObjectMap> vertex) throws Exception {
    MergeTuple tuple = new MergeTuple();
    ObjectMap properties = vertex.getValue();
//    LOG.info("PROPERTIES: " + properties.toString() + " " + vertex.getId());
    tuple.setId(vertex.getId());
    tuple.setLabel(properties.getLabel());
    tuple.setLatitude(properties.getLatitude());
    tuple.setLongitude(properties.getLongitude());
    tuple.setIntTypes(properties.getIntTypes());
    tuple.setIntSources(properties.getIntSources());
    tuple.addClusteredElements(properties.getVerticesList());
    tuple.setBlockingLabel(Utils.getBlockingLabel(properties.getLabel()));

//    LOG.info("CREATE: " + tuple.toString());
    return tuple;
  }
}
