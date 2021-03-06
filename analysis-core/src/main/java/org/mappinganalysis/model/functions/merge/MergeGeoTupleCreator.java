package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
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
  private BlockingStrategy blockingStrategy;

  /**
   * Default Constructor standard blocking
   */
  public MergeGeoTupleCreator() {
    blockingStrategy = BlockingStrategy.STANDARD_BLOCKING;
  }

  /**
   * Constructor for variable blocking strategy
   * @param strategy blocking strategy
   */
  public MergeGeoTupleCreator(BlockingStrategy strategy) {
    blockingStrategy = strategy;
  }

  @Override
  public MergeGeoTuple map(Vertex<Long, ObjectMap> vertex) throws Exception {
    MergeGeoTuple tuple = new MergeGeoTuple();

    ObjectMap properties = vertex.getValue();
    properties.setMode(Constants.GEO);

    tuple.setId(vertex.getId());
    tuple.setLabel(properties.getLabel());

    if (properties.hasGeoPropertiesValid()) {
      tuple.setLatitude(properties.getLatitude());
      tuple.setLongitude(properties.getLongitude());
    }
    tuple.setIntTypes(properties.getIntTypes());

    tuple.setIntSources(properties.getIntDataSources());
    tuple.addClusteredElements(properties.getVerticesList());
    tuple.setBlockingLabel(Utils.getBlockingKey(
        blockingStrategy,
        Constants.GEO,
        properties.getLabel(),
        0));

    return tuple;
  }
}