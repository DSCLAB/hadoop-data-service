/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.hdsb.core;

import java.io.IOException;

/**
 *
 * @author Weli
 */
public class UserInput {

  private final String configPath;

  public UserInput(String[] args) {
    this.configPath = args[0];
  }

  public String getConfigPath() throws IOException {
    if (configPath == null) {
      throw new IOException("Usage:bash <bin> <configPath>");
    }
    return configPath;
  }

}
