package org.mappinganalysis.io.output;

import org.apache.flink.api.java.tuple.Tuple3;

public class VertexLabelString extends Tuple3<Long, String, String> {

  public VertexLabelString() {
  }

  public VertexLabelString(Long id, String label, String value) {
    this.f0 = id;
    this.f1 = label;
    this.f2 = value;
  }

  public String getValue() {
    return this.f2;
  }

  public void setValue(String value) {
    this.f2 = value;
  }

  public Long getId() {
    return this.f0;
  }

  public String getLabel() {
    return this.f1;
  }

  public void setLabel(String label) {
    this.f1 = label;
  }
}
