/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.parser;

/**
 *
 * @author brandboat
 * @param <U>
 * @param <T>
 */
public class RangeValue<U, T> {

  private U minValue;
  private T maxValue;

  public RangeValue() {
  }

  public RangeValue(U minValue, T maxValue) {
    this.minValue = minValue;
    this.maxValue = maxValue;
  }

  public void setMinValue(U minValue) {
    this.minValue = minValue;
  }

  public void setMaxValue(T maxValue) {
    this.maxValue = maxValue;
  }

  public U getMinValue() {
    return minValue;
  }

  public T getMaxValue() {
    return maxValue;
  }

  public boolean isRangeFilled() {
    if (minValue == null || maxValue == null) {
      return false;
    }
    return true;
  }
}
