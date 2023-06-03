/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.datastorage.status;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Optional;

/**
 *
 * @author brandboat
 */
public class ErrorInfo {

  private final String className;
  private final String exceptionName;
  private final String errorMessage;
  private final static String PACKAGE_NAME = "umc.cdc.hds";

  /**
   * Get ExceptionName, className that exception occurs, and errormessage. If we
   * cannot find any classname that have hds package name in the exception, the
   * errormessage will be all stacktrace.
   *
   * @param ex
   */
  public ErrorInfo(Exception ex) {
    Optional<StackTraceElement> hdsErrorOccurStack = getErrorOccurStack(ex);
    if (hdsErrorOccurStack.isPresent()) {
      this.className = hdsErrorOccurStack.get().getClassName();
      this.errorMessage = new StringBuilder(
          errorMessageThreshold(ex.getMessage()))
          .append(" at line ")
          .append(hdsErrorOccurStack.get().getLineNumber())
          .toString();
    } else {
      StringWriter errors = new StringWriter();
      ex.printStackTrace(new PrintWriter(errors));
      this.className = null;
      this.errorMessage = errors.toString();
    }
    this.exceptionName = ex.getClass().getName();
  }

  private String errorMessageThreshold(String err) {
    if (err.length() >= 500) {
      err = err.substring(0, 500) + "...";
    }
    return err;
  }

  public String getClassName() {
    return className;
  }

  public String getExceptionName() {
    return exceptionName;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  private Optional<StackTraceElement> getErrorOccurStack(Exception ex) {
    for (StackTraceElement ste : ex.getStackTrace()) {
      if (ste.getClassName().contains(PACKAGE_NAME)) {
        return Optional.of(ste);
      }
    }
    return Optional.empty();
  }
}
