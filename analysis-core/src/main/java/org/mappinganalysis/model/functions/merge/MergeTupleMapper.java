package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

public class MergeTupleMapper implements FlatMapFunction<Vertex<Long, ObjectMap>, MergeTuple> {
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

    if (properties.containsKey(Constants.COMP_TYPE)) {
      for (String type : properties.getTypes(Constants.COMP_TYPE)) {
        reuseTuple.setType(type);
        out.collect(reuseTuple);
      }
    }
  }

}
