package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple4;

public class EdgeIdsVertexValueTuple extends Tuple4<Long, Long, ObjectMap, ObjectMap> {
  public EdgeIdsVertexValueTuple() {
  }

  public EdgeIdsVertexValueTuple(Long srcId, Long trgId, ObjectMap srcMap, ObjectMap trgMap) {
    super(srcId, trgId, srcMap, trgMap);
  }

  public Long getSrcId() {
    return f0;
  }

  public Long getTrgId() {
    return f1;
  }

  public ObjectMap getSrcMap() {
    return f2;
  }

  public ObjectMap getTrgMap() {
    return f3;
  }

  public void checkSideAndUpdate(int side, ObjectMap value) {
    if (getSrcMap().isEmpty() && getTrgMap().isEmpty()) {
      if (side == 0) {
        this.f2 = value;
      } else  if (side == 1){
        this.f3 = value;
      }
    } else if (getSrcMap().isEmpty() && side == 0) {
      this.f2 = value;
    } else if (getTrgMap().isEmpty() && side == 1) {
      this.f3 = value;
    }

  }
}
