package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.TypeDictionary;
import org.mappinganalysis.util.Utils;

public class MergeTupleMapper implements FlatMapFunction<Vertex<Long, ObjectMap>, MergeTuple> {
  private static final Logger LOG = Logger.getLogger(MergeTupleMapper.class);

  MergeTuple reuseTuple;

  public MergeTupleMapper() {
    this.reuseTuple = new MergeTuple();
  }
  @Override
  public void flatMap(Vertex<Long, ObjectMap> vertex, Collector<MergeTuple> out) throws Exception {
    ObjectMap properties = vertex.getValue();
    reuseTuple.setVertexId(vertex.getId());
    reuseTuple.setSize(properties.getVerticesCount());
    reuseTuple.setIntSources(properties.getSourcesAsInt());
    reuseTuple.setLabel(Utils.getBlockingLabel(properties.getLabel()));

    if (vertex.getId() == 1981L || vertex.getId() == 1982L) {
      LOG.info("###wei " + vertex.toString());
    }
    // no type entities get associated to every block,
    // normal entities only to blocks where they have the type

    reuseTuple.setType("temp");
    out.collect(reuseTuple);

    // todo wip
//    if (properties.hasTypeNoType(Constants.COMP_TYPE)) {
//      for (String type : TypeDictionary.SHADED_TYPES) {
//        reuseTuple.setType(type);
//        out.collect(reuseTuple);
//      }
//    } else {
//      for (String type : properties.getTypes(Constants.COMP_TYPE)) {
//        reuseTuple.setType(type);
//        out.collect(reuseTuple);
//      }
//    }
  }
}
