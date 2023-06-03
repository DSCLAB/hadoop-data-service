/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.parser;

import java.io.IOException;

/**
 *
 * @author brandboat
 */
public class ElaboratedPositiveInteger extends ElaboratedValue<Integer> {

  private final Integer v;

  public ElaboratedPositiveInteger(String s) throws IOException {
    super(s);
    v = parseInt(s);
  }

  private Integer parseInt(String s) throws IOException {
    Integer val = null;
    try {
      val = Integer.valueOf(s);
      if (val < 0) {
        throw new IOException("Value should not less than 0.");
      }
    } catch (NumberFormatException ex) {
      if (s != null && !s.isEmpty()) {
        throw new IOException(ex);
      }
    }
    return val;
  }

  @Override
  protected Integer getValue() {
    return v;
  }

}
