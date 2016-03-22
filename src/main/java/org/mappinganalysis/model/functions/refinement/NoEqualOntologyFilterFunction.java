package org.mappinganalysis.model.functions.refinement;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

import java.util.Set;

public class NoEqualOntologyFilterFunction implements FilterFunction<Triplet<Long, ObjectMap, NullValue>> {
  @Override
  public boolean filter(Triplet<Long, ObjectMap, NullValue> triplet) throws Exception {
    Set<String> srcOnts = (Set<String>) triplet.getSrcVertex().getValue().get(Utils.ONTOLOGIES);
    Set<String> trgOnts = (Set<String>) triplet.getTrgVertex().getValue().get(Utils.ONTOLOGIES);

    for (String srcValue : srcOnts) {
      if (trgOnts.contains(srcValue)) {
        return false;
      }
    }
    return true;
  }
}
