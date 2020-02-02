/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Connection.
 */
public class Connection implements java.sql.Connection {

  protected final String host;
  protected final int port;

  public Connection(String host, int port) {
    this.host = host;
    this.port = port;
  }

  @Override
  public Statement createStatement() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String nativeSQL(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setAutoCommit(boolean b) throws SQLException {

  }

  @Override
  public void commit() throws SQLException {

  }

  @Override
  public void rollback() throws SQLException {

  }

  @Override
  public void close() throws SQLException {

  }

  @Override
  public boolean isClosed() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setReadOnly(boolean b) throws SQLException {

  }

  @Override
  public String getCatalog() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setCatalog(String s) throws SQLException {

  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setTransactionIsolation(int i) throws SQLException {

  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void clearWarnings() throws SQLException {

  }

  @Override
  public Statement createStatement(int i, int i1) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String s, int i, int i1) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(String s, int i, int i1) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setHoldability(int i) throws SQLException {

  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Savepoint setSavepoint(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {

  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {

  }

  @Override
  public Statement createStatement(int i, int i1, int i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String s, int i, int i1, int i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(String s, int i, int i1, int i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String s, int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String s, int[] ints) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String s, String[] strings) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isValid(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setClientInfo(String s, String s1) throws SQLClientInfoException {

  }

  @Override
  public String getClientInfo(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {

  }

  @Override
  public Array createArrayOf(String s, Object[] objects) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Struct createStruct(String s, Object[] objects) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getSchema() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setSchema(String s) throws SQLException {

  }

  @Override
  public void abort(Executor executor) throws SQLException {

  }

  @Override
  public void setNetworkTimeout(Executor executor, int i) throws SQLException {

  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T unwrap(Class<T> aClass) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
}
