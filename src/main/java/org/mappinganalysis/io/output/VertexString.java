/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */
package org.mappinganalysis.io.output;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * (vertexId, label)
 */
public class VertexString extends Tuple2<Long, String> {

  /**
   * default constructor
   */
  public VertexString() {
  }

  /**
   * constructor with field values
   * @param id vertex id
   * @param label vertex label
   */
  public VertexString(Long id, String label) {
    this.f0 = id;
    this.f1 = label;
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
