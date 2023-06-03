/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.parser;

/**
 *
 * @author brandboat
 */
public class ElaboratedString extends ElaboratedValue<String> {

  private final String elaboratedStr;

  public ElaboratedString(final String str) {
    super(str);
    elaboratedStr = str;
  }

  @Override
  protected String getValue() {
    return elaboratedStr;
  }

}
