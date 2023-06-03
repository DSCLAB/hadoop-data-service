/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.httpserver;

import java.io.Closeable;
import java.io.IOException;

/**
 *
 * @author brandboat
 */
public interface Formatter extends Closeable {

  public void convert(Result result) throws IOException;

}
