/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.core;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author brandboat
 */
public interface Configurable {

  public Configuration getConfig();
}
