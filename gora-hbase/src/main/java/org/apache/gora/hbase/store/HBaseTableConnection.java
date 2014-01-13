/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gora.hbase.store;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.InterruptedIOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.regionserver.HRegion.RowLock;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

//org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

/**
 * Thread safe implementation to connect to a HBase table.
 *
 */
public class HBaseTableConnection implements HTableInterface {
  /*
   * The current implementation uses ThreadLocal HTable instances. It keeps
   * track of the floating instances in order to correctly flush and close
   * the connection when it is closed. HBase itself provides a utility called
   * HTablePool for maintaining a pool of tables, but there are still some
   * drawbacks that are only solved in later releases.
   * 
   */
  
  private final Configuration conf;
  private final ThreadLocal<HTable> tables;
  private final BlockingQueue<HTable> pool = new LinkedBlockingQueue<HTable>();
  private boolean autoflush;
  private final String tableName;
  
  /**
   * Instantiate new connection.
   * 
   * @param conf
   * @param tableName
   * @param autoflush
   * @throws IOException
   */
  public HBaseTableConnection(Configuration conf, String tableName, boolean autoflush)
      throws IOException {
    this.conf = conf;
    this.tables = new ThreadLocal<HTable>();
    this.tableName = tableName;
    this.autoflush = autoflush;
  }
  
  private HTable getTable() {
    HTable table = tables.get();
    if (table == null) {
        try {
            table = new HTable(conf, tableName) {            
                    @Override
                    public synchronized void flushCommits()  {
                        try {                
                            super.flushCommits();
                        } catch (Exception iioe) {
                            throw new RuntimeException(iioe);
                        }
                    }
                };
        } catch(IOException ioe) {
            throw new RuntimeException(ioe);
        }

      table.setAutoFlush(autoflush);
      pool.add(table); //keep track
      tables.set(table);
    }
    return table;
  }
  
  @Override
  public void close() throws IOException {
    // Flush and close all instances.
    // (As an extra safeguard one might employ a shared variable i.e. 'closed'
    //  in order to prevent further table creation but for now we assume that
    //  once close() is called, clients are no longer using it).
    for (HTable table : pool) {
      table.flushCommits();
      table.close();
    }
  }

  @Override
  public byte[] getTableName() {
    return Bytes.toBytes(tableName);
  }

  @Override
  public TableName getName() 
  {
      return getTable().getName();
  }
    

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public boolean isAutoFlush() {
    return autoflush;
  }

  @Override
  public void setAutoFlushTo(boolean autoFlush) 
  {
      this.autoflush = autoFlush;
      for (HTable table : pool) {
          table.setAutoFlushTo(autoFlush);
      }
  }

  @Override
  public void setAutoFlush(boolean autoFlush) 
  {
      setAutoFlushTo(autoFlush);
  }

  @Override
  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) 
  {
      this.autoflush = autoFlush;
      for (HTable table : pool) {
          table.setAutoFlush(autoFlush, clearBufferOnFail);
      }
  }
    
  /**
   * getStartEndKeys provided by {@link HTable} but not {@link HTableInterface}.
   * @see HTable#getStartEndKeys()
   */
  public Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
    return getTable().getStartEndKeys();
  }
  /**
   * getRegionLocation provided by {@link HTable} but not 
   * {@link HTableInterface}.
   * @see HTable#getRegionLocation(byte[])
   */
  public HRegionLocation getRegionLocation(final byte[] bs) throws IOException {
    return getTable().getRegionLocation(bs);
  }

  @Override
  public void setWriteBufferSize(long writeBufferSize)  throws IOException
  {
      getTable().setWriteBufferSize(writeBufferSize);
  }

  @Override
  public long getWriteBufferSize() 
  {
      return getTable().getWriteBufferSize();
  }
    
  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    return getTable().getTableDescriptor();
  }

  @Override
  public boolean exists(Get get) throws IOException {
    return getTable().exists(get);
  }

  @Override
  public Boolean[] exists(List<Get> gets) throws IOException 
  {
      return getTable().exists(gets);
  }

  @Override
  public void batch(List<? extends Row> actions, Object[] results) throws IOException,
      InterruptedException {
    getTable().batch(actions, results);
  }

  @Override
  public Object[] batch(List<? extends Row> actions) throws IOException,
      InterruptedException {
    return getTable().batch(actions);
  }

    @Override
    public <R> void batchCallback(
    final List<? extends Row> actions, final Object[] results, final Batch.Callback<R> callback)
      throws IOException, InterruptedException 
    {
        getTable().batchCallback(actions, results, callback);
    }
    


  @Override
  public <R> Object[] batchCallback(
                                    List<? extends Row> actions, Batch.Callback<R> callback
                                    ) throws IOException,
                                             InterruptedException 
    {
        return getTable().batchCallback(actions, callback);
    }
    

  @Override
  public Result get(Get get) throws IOException {
    return getTable().get(get);
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    return getTable().get(gets);
  }

  @Override
  public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
    return getTable().getRowOrBefore(row, family);
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    return getTable().getScanner(scan);
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    return getTable().getScanner(family);
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier)
      throws IOException {
    return getTable().getScanner(family, qualifier);
  }

  @Override
  public void put(Put put) throws IOException {
    getTable().put(put);
  }

  @Override
  public void put(List<Put> puts) throws IOException {
    getTable().put(puts);
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Put put) throws IOException {
    return getTable().checkAndPut(row, family, qualifier, value, put);
  }

  @Override
  public void delete(Delete delete) throws IOException {
    getTable().delete(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    getTable().delete(deletes);
    
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Delete delete) throws IOException {
    return getTable().checkAndDelete(row, family, qualifier, value, delete);
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    return getTable().increment(increment);
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount) throws IOException {
    return getTable().incrementColumnValue(row, family, qualifier, amount);
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount, boolean writeToWAL) throws IOException {
    return getTable().incrementColumnValue(row, family, qualifier, amount,
        writeToWAL);
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
                                   long amount, Durability durability) throws IOException {
      return getTable().incrementColumnValue(row, family, qualifier, amount, durability);
  }

  @Override
  public void flushCommits() throws IOException {
    for (HTable table : pool) {
      table.flushCommits();
    }
  }

    /*
  @Override
  public RowLock lockRow(byte[] row) throws IOException {
    return getTable().lockRow(row);
  }

  @Override
  public void unlockRow(RowLock rl) throws IOException {
    getTable().unlockRow(rl);
  }
    */

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] row) 
    {
        return getTable().coprocessorService(row);
    }
    
    @Override
    public <T extends Service, R> Map<byte[],R> coprocessorService(final Class<T> service,
                                                            byte[] startKey, 
                                                            byte[] endKey, 
                                                            final Batch.Call<T,R> callable)
        throws ServiceException, Throwable 
    {
        return         getTable().coprocessorService(service,
                                                     startKey, 
                                                     endKey, 
                                                     callable);
    }
    
    @Override
    public <T extends Service, R> void coprocessorService(final Class<T> service,
                                                   byte[] startKey, 
                                                   byte[] endKey, 
                                                   final Batch.Call<T,R> callable,
                                                   final Batch.Callback<R> callback) 
        throws ServiceException, Throwable 
    {
        getTable().coprocessorService(service,
                                      startKey, 
                                      endKey, 
                                      callable,
                                      callback);
    }

    @Override
    public Result append(final Append append) throws IOException 
    {
        return getTable().append(append);
    }

    @Override
    public void mutateRow(final RowMutations rm) throws IOException 
    {
        getTable().mutateRow(rm);
    }
}
