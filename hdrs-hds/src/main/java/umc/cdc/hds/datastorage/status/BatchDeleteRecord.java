/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage.status;

/**
 *
 * @author brandboat
 */
public class BatchDeleteRecord extends DataRecord {

  private final ErrorInfo error;

  public BatchDeleteRecord(DataRecord dr, ErrorInfo error) {
    super(dr.getUri(), dr.getLocation(), dr.getName(), dr.getSize(),
        dr.getTime(), dr.getFileType().orElse(null), dr.getDataOwner());
    this.error = error;
  }

  public String getErrorMessage() {
    return error.getErrorMessage();
  }
}
