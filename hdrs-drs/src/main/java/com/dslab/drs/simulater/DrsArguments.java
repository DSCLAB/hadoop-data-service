package com.dslab.drs.simulater;

import org.apache.hadoop.fs.Path;

/**
 *
 * @author kh87313
 */
public class DrsArguments {

  private String code;
  private String data;
  private String config;
  private String codeout;
  private String copyto;
  private String consoleto;

  public DrsArguments(String code, String data, String config, String codeout, String copyto) {
    this(code, data, config, codeout, copyto, null);
  }

  public DrsArguments(String code, String data, String config, String codeout, String copyto, String consoleto) {
    this.code = code;
    this.data = data;
    this.config = config;
    this.codeout = codeout;
    this.copyto = copyto;
    this.consoleto = consoleto;
  }

  public String getCode() {
    return code;
  }

  public String getData() {
    return data;
  }

  public String getConfig() {
    return config;
  }

  public String getCodeout() {
    return codeout;
  }

  public String getCopyto() {
    return copyto;
  }

  public String getConsoleto() {
    return consoleto;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public void setData(String data) {
    this.data = data;
  }

  public void setConfig(String config) {
    this.config = config;
  }

  public void setCodeout(String codeout) {
    this.codeout = codeout;
  }

  public void setCopyto(String copyto) {
    this.copyto = copyto;
  }

  public void setConsoleto(String consoleto) {
    this.consoleto = consoleto;
  }
  public String toString(){
    return "code="+code+",data="+data+",config="+config+",codeout="+codeout+",copyto="+copyto+",consoleto="+consoleto;
  }
}
