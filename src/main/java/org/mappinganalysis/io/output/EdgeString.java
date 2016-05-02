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

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * (sourceId, targetId, sourceLabel, targetLabel)
 */
public class EdgeString extends Tuple4<Long, Long, String, String> {

  /**
   * default constructor
   */
  public EdgeString() {
  }

  /**
   * constructor with field values
   * @param sourceId source vertex id
   * @param targetId target vertex id
   */
  public EdgeString(Long sourceId, Long targetId) {

    this.f0 = sourceId;
    this.f1 = targetId;
    this.f2 = "";
    this.f3 = "";
  }

  public Long getSourceId() {
    return this.f0;
  }

  public Long getTargetId() {
    return this.f1;
  }

  public String getSourceLabel() {
    return this.f2;
  }

  public void setSourceLabel(String sourceLabel) {
    this.f2 = sourceLabel;
  }

  public String getTargetLabel() {
    return this.f3;
  }

  public void setTargetLabel(String targetLabel) {
    this.f3 = targetLabel;
  }
}
