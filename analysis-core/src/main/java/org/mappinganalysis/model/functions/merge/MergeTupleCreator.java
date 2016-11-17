package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Utils;

class MergeTupleCreator implements MapFunction<Vertex<Long, ObjectMap>, MergeTuple> {
  MergeTuple reuseTuple;
  private static final Logger LOG = Logger.getLogger(MergeTupleCreator.class);

  public MergeTupleCreator() {
    this.reuseTuple = new MergeTuple();
  }

  @Override
  public MergeTuple map(Vertex<Long, ObjectMap> vertex) throws Exception {
    ObjectMap properties = vertex.getValue();
    LOG.info(properties.toString());
    reuseTuple.setId(vertex.getId());
    reuseTuple.setLabel(properties.getLabel());
    reuseTuple.setLatitude(properties.getLatitude());
    reuseTuple.setLongitude(properties.getLongitude());
    reuseTuple.setIntTypes(properties.getIntTypes());
    reuseTuple.setIntSources(properties.getIntSources());
    reuseTuple.addClusteredElements(properties.getVerticesList());
    reuseTuple.setBlockingLabel(Utils.getBlockingLabel(properties.getLabel()));

    return reuseTuple;
  }
}
