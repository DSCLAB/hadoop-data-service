/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package umc.cdc.hds.mapping;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import umc.cdc.hds.core.HBaseConnection;
import umc.cdc.hds.core.HDSConstants;
import umc.cdc.hds.core.HDSUtil;
import umc.cdc.hds.core.SingletonConnectionFactory;
import umc.cdc.hds.uri.Query;

/**
 *
 * @author brandboat
 */
public class Mapping implements Closeable {

  private final HBaseConnection conn;
  private boolean isMappingTableCreated = false;

  public Mapping(Configuration conf) throws IOException {
    this.conn = SingletonConnectionFactory.createConnection(conf);
  }

  private void createMappingTableIfNotExisted(TableName tableName) throws IOException {
    try (Admin admin = conn.getAdmin()) {
      if (admin.tableExists(tableName)) {
        return;
      }
      HDSUtil.createNamespaceIfNotExisted(conn, tableName.getNamespaceAsString());
      HTableDescriptor tableDesc = new HTableDescriptor(tableName);
      HColumnDescriptor colDesc = new HColumnDescriptor(
          HDSConstants.DEFAULT_FAMILY);
      colDesc.setCompressionType(Compression.Algorithm.GZ);
      colDesc.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
      tableDesc.addFamily(colDesc);
      admin.createTable(tableDesc);
    } catch (TableExistsException ex) {
    } finally {
      isMappingTableCreated = true;
    }
  }

  private Table getMappingTable() throws IOException {
    if (!isMappingTableCreated) {
      createMappingTableIfNotExisted(HDSConstants.MAPPING_TABLE);
    }
    return conn.getTable(HDSConstants.MAPPING_TABLE);
  }

  public AccountInfo add(final AccountQuery accountQuery) throws IOException {
    Put put = accountQuery.getId().map(v -> new Put(Bytes.toBytes(v)))
        .orElseThrow(() -> new IOException("Cannot find id."));
    Delete delete = new Delete(put.getRow());
    accountQuery.getDomain().ifHasValue(v -> {
      put.addColumn(HDSConstants.DEFAULT_FAMILY,
          HDSConstants.MAPPING_DOMAIN_QUALIFIER,
          Bytes.toBytes(v));
    }).ifEmpty(() -> {
      delete.addColumns(HDSConstants.DEFAULT_FAMILY,
          HDSConstants.MAPPING_DOMAIN_QUALIFIER);
    });
    accountQuery.getHost().ifHasValue(v -> {
      put.addColumn(HDSConstants.DEFAULT_FAMILY,
          HDSConstants.MAPPING_HOST_QUALIFIER,
          Bytes.toBytes(v));
    }).ifEmpty(() -> {
      delete.addColumns(HDSConstants.DEFAULT_FAMILY,
          HDSConstants.MAPPING_HOST_QUALIFIER);
    });
    accountQuery.getPort().ifHasValue(v -> {
      put.addColumn(HDSConstants.DEFAULT_FAMILY,
          HDSConstants.MAPPING_PORT_QUALIFIER,
          Bytes.toBytes(v));
    }).ifEmpty(() -> {
      delete.addColumns(HDSConstants.DEFAULT_FAMILY,
          HDSConstants.MAPPING_PORT_QUALIFIER);
    });
    accountQuery.getUser().ifHasValue(v -> {
      put.addColumn(HDSConstants.DEFAULT_FAMILY,
          HDSConstants.MAPPING_USER_QUALIFIER,
          Bytes.toBytes(v));
    }).ifEmpty(() -> {
      delete.addColumns(HDSConstants.DEFAULT_FAMILY,
          HDSConstants.MAPPING_USER_QUALIFIER);
    });
    accountQuery.getPasswd().ifHasValue(v -> {
      put.addColumn(HDSConstants.DEFAULT_FAMILY,
          HDSConstants.MAPPING_PASSWORD_QUALIFIER,
          Bytes.toBytes(v));
    }).ifEmpty(() -> {
      delete.addColumns(HDSConstants.DEFAULT_FAMILY,
          HDSConstants.MAPPING_PASSWORD_QUALIFIER);
    });

    try (Table table = getMappingTable()) {
      if (!put.isEmpty()) {
        table.put(put);
      }
      if (!delete.isEmpty()) {
        table.delete(delete);
      }
    } catch (IOException e) {
      throw new IOException("Failed to open mapping table", e);
    }

    return find(new String(put.getRow()))
        .orElseThrow(() -> new IOException("Cannnot find account info."));
  }

  public Optional<AccountInfo> find(final String id) throws IOException {
    try (Table table = getMappingTable()) {
      Get get = new Get(Bytes.toBytes(id));
      get.addFamily(HDSConstants.DEFAULT_FAMILY);
      Result result = table.get(get);
      if (result.isEmpty()) {
        return Optional.empty();
      }
      AccountInfo accountInfo = toAccountInfo(result);
      return Optional.of(accountInfo);
    } catch (IOException e) {
      throw new IOException("Failed to open mapping table", e);
    }
  }

  public Iterator<AccountInfo> delete(final Query query) throws IOException {
    try (Table table = getMappingTable()) {
      List<AccountInfo> infos = new LinkedList<>();
      Scan scan = createScan(query);
      try (ResultScanner rs = table.getScanner(scan)) {
        for (Result result : rs) {
          if (result == null) {
            break;
          }
          table.delete(new Delete(result.getRow()));
          infos.add(toAccountInfo(result));
        }
      }
      return infos.iterator();
    }
  }

  public Iterator<AccountInfo> list(final Query query)
      throws IOException {
    try (Table table = getMappingTable()) {
      List<AccountInfo> infos = new LinkedList<>();
      Scan scan = createScan(query);
      try (ResultScanner rs = table.getScanner(scan)) {
        int index = -1;
        for (Result result : rs) {
          if (result == null) {
            break;
          }
          index++;
          if (index < query.getOffset()) {
            continue;
          }
          if (infos.size() >= query.getLimit()) {
            return infos.iterator();
          }
          infos.add(toAccountInfo(result));
        }
      }
      return infos.iterator();
    }
  }

  private Scan createScan(Query query) {
    Scan scan = new Scan();
    List<Filter> filters = new LinkedList<>();
    query.getQueryValue(HDSConstants.MAPPING_ARG_ID).ifPresent(v -> {
      filters.add(new RowFilter(
          CompareFilter.CompareOp.EQUAL,
          new RegexStringComparator(
              HDSUtil.convertWildCardToRegEx(v))));
    });
    query.getQueryValue(HDSConstants.MAPPING_ARG_DOMAIN).ifPresent(v -> {
      SingleColumnValueFilter filter = new SingleColumnValueFilter(
          HDSConstants.DEFAULT_FAMILY,
          HDSConstants.MAPPING_DOMAIN_QUALIFIER,
          CompareFilter.CompareOp.EQUAL,
          new RegexStringComparator(
              HDSUtil.convertWildCardToRegEx(v)));
      filter.setFilterIfMissing(true);
      filters.add(filter);
    });
    query.getQueryValue(HDSConstants.MAPPING_ARG_HOST).ifPresent(v -> {
      SingleColumnValueFilter filter = new SingleColumnValueFilter(
          HDSConstants.DEFAULT_FAMILY,
          HDSConstants.MAPPING_HOST_QUALIFIER,
          CompareFilter.CompareOp.EQUAL,
          new RegexStringComparator(
              HDSUtil.convertWildCardToRegEx(v)));
      filter.setFilterIfMissing(true);
      filters.add(filter);
    });
    Optional<Integer> port = query.getQueryValueAsPositiveInteger(
        HDSConstants.MAPPING_ARG_PORT);
    if (port.isPresent()) {
      SingleColumnValueFilter filter = new SingleColumnValueFilter(
          HDSConstants.DEFAULT_FAMILY,
          HDSConstants.MAPPING_PORT_QUALIFIER,
          CompareFilter.CompareOp.EQUAL,
          new BinaryComparator(Bytes.toBytes(port.get())));
      filter.setFilterIfMissing(true);
      filters.add(filter);
    } else {
      query.getQueryValue(HDSConstants.MAPPING_ARG_MINPORT).ifPresent(v -> {
        int val;
        try {
          val = Integer.parseInt(v);
          if (val < 0) {
            val = 0;
          }
        } catch (NumberFormatException ex) {
          val = 0;
        }
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
            HDSConstants.DEFAULT_FAMILY,
            HDSConstants.MAPPING_PORT_QUALIFIER,
            CompareFilter.CompareOp.GREATER_OR_EQUAL,
            new BinaryComparator(Bytes.toBytes(val)));
        filter.setFilterIfMissing(true);
        filters.add(filter);
      });
      query.getQueryValue(HDSConstants.MAPPING_ARG_MAXPORT).ifPresent(v -> {
        int val;
        try {
          val = Integer.parseInt(v);
          if (val < 0) {
            val = Integer.MAX_VALUE;
          }
        } catch (NumberFormatException ex) {
          val = Integer.MAX_VALUE;
        }
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
            HDSConstants.DEFAULT_FAMILY,
            HDSConstants.MAPPING_PORT_QUALIFIER,
            CompareFilter.CompareOp.LESS_OR_EQUAL,
            new BinaryComparator(Bytes.toBytes(val)));
        filter.setFilterIfMissing(true);
        filters.add(filter);
      });
    }
    query.getQueryValue(HDSConstants.MAPPING_ARG_USER).ifPresent(v -> {
      SingleColumnValueFilter filter = new SingleColumnValueFilter(
          HDSConstants.DEFAULT_FAMILY,
          HDSConstants.MAPPING_USER_QUALIFIER,
          CompareFilter.CompareOp.EQUAL,
          new RegexStringComparator(
              HDSUtil.convertWildCardToRegEx(v)));
      filter.setFilterIfMissing(true);
      filters.add(filter);
    });
    query.getQueryValue(HDSConstants.MAPPING_ARG_PASSWD).ifPresent(v -> {
      SingleColumnValueFilter filter = new SingleColumnValueFilter(
          HDSConstants.DEFAULT_FAMILY,
          HDSConstants.MAPPING_PASSWORD_QUALIFIER,
          CompareFilter.CompareOp.EQUAL,
          new RegexStringComparator(
              HDSUtil.convertWildCardToRegEx(v)));
      filter.setFilterIfMissing(true);
      filters.add(filter);
    });
    if (!filters.isEmpty()) {
      scan.setFilter(new FilterList(filters));
    }
    return scan;
  }

  private AccountInfo toAccountInfo(final Result result) {
    byte[] domainBytes = result.getValue(
        HDSConstants.DEFAULT_FAMILY,
        HDSConstants.MAPPING_DOMAIN_QUALIFIER);
    byte[] hostBytes = result.getValue(
        HDSConstants.DEFAULT_FAMILY,
        HDSConstants.MAPPING_HOST_QUALIFIER);
    byte[] portBytes = result.getValue(
        HDSConstants.DEFAULT_FAMILY,
        HDSConstants.MAPPING_PORT_QUALIFIER);
    byte[] userBytes = result.getValue(
        HDSConstants.DEFAULT_FAMILY,
        HDSConstants.MAPPING_USER_QUALIFIER);
    byte[] passwdBytes = result.getValue(
        HDSConstants.DEFAULT_FAMILY,
        HDSConstants.MAPPING_PASSWORD_QUALIFIER);
    String domain = (domainBytes == null) ? null : Bytes.toString(domainBytes);
    String host = (hostBytes == null) ? null : Bytes.toString(hostBytes);
    Integer port = (portBytes == null) ? null : Bytes.toInt(portBytes);
    String user = (userBytes == null) ? null : Bytes.toString(userBytes);
    String passwd = (passwdBytes == null) ? null : Bytes.toString(passwdBytes);

    AccountInfo accountInfo = new AccountInfo(
        Bytes.toString(result.getRow()), domain, host, port, user, passwd);
    return accountInfo;
  }

  @Override
  public void close() throws IOException {
    conn.close();
  }
}
