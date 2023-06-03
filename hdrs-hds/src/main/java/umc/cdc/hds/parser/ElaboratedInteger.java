/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.parser;

import java.io.IOException;
import java.util.Optional;

/**
 *
 * @author brandboat
 */
public class ElaboratedInteger extends ElaboratedValue<Integer> {

  private final Integer v;

  public ElaboratedInteger(final String s) throws IOException {
    super(s);
    v = parseInteger(s);
  }

  private Integer parseInteger(String s) throws IOException {
    try {
      return Integer.parseInt(s);
    } catch (NumberFormatException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected Integer getValue() {
    return v;
  }

}
