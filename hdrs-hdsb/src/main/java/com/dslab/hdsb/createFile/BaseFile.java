/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.hdsb.createFile;

import java.io.IOException;
import java.util.Queue;

/**
 *
 * @author Weli
 */
public interface BaseFile {

  void create(String resource, int i) throws IOException;

}
