/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.adapter.jdbc;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * POJO to handle the YAML data from the test YAML file.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Table {

    private String name;
    private String create;
    private String[] data;
    private String query;
    private String drop;
	private int [] ints;
	private int [] booleans;
	private int [] tinyInts;
	private int [] smallInts;
	private int [] bigInts;
	private long [] times;
	private long [] dates;
	private long [] timestamps;
	private String [] bytes;
	private String [] varchars; 
	private String [] chars;
	private String [] clobs;
	private float [] reals;
	private double [] doubles;
	private BigDecimal [] decimals;
	private String allColumns;	
	private long [] pstTime;
	private long [] estTime;
	private long [] gmtTime;
	private long [] pstDate;
	private long [] estDate;
	private long [] gmtDate;
	private long [] pstTimestamp;
	private long [] estTimestamp;
	private long [] gmtTimestamp;
	
	public Table() {
    }

	public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getCreate() {
        return create;
    }
    public void setCreate(String create) {
        this.create = create;
    }
    public String[] getData() {
        return data;
    }
    public void setData(String[] data) {
        this.data = data;
    }
    public String getQuery() {
        return query;
    }
    public void setQuery(String query) {
        this.query = query;
    }
    public String getDrop() {
        return drop;
    }
    public void setDrop(String drop) {
        this.drop = drop;
    }
	public int[] getInts() {
		return ints;
	}
	public void setInts(int[] ints) {
		this.ints = ints;
	}
	public int[] getBooleans() {
		return booleans;
	}
	public void setBooleans(int[] booleans) {
		this.booleans = booleans;
	}
	public BigDecimal [] getDecimals() {
		return decimals;
	}
	public void setDecimals(BigDecimal [] decimals) {
		this.decimals = decimals;
	}
	public double[] getDoubles() {
		return doubles;
	}
	public void setDoubles(double[] doubles) {
		this.doubles = doubles;
	}
	public int[] getTinyInts() {
		return tinyInts;
	}
	public void setTinyInts(int[] tinyInts) {
		this.tinyInts = tinyInts;
	}
	public int[] getSmallInts() {
		return smallInts;
	}
	public void setSmallInts(int[] smallInts) {
		this.smallInts = smallInts;
	}
	public int[] getBigInts() {
		return bigInts;
	}
	public void setBigInts(int[] bigInts) {
		this.bigInts = bigInts;
	}
	public long[] getTimes() {
		return times;
	}
	public void setTimes(long[] times) {
		this.times = times;
	}
	public long[] getDates() {
		return dates;
	}
	public void setDates(long[] dates) {
		this.dates = dates;
	}
	public long[] getTimestamps() {
		return timestamps;
	}
	public void setTimestamps(long[] timestamps) {
		this.timestamps = timestamps;
	}
	public String [] getBytes() {
		return bytes;
	}
	public void setBytes(String [] bytes) {
		this.bytes = bytes;
	}
	public String []getVarchars() {
		return varchars;
	}
	public void setVarchars(String [] varchars) {
		this.varchars = varchars;
	}
	public String [] getChars() {
		return chars;
	}
	public void setChars(String [] chars) {
		this.chars = chars;
	}
	public String [] getClobs() {
		return clobs;
	}
	public void setClobs(String [] clobs) {
		this.clobs = clobs;
	}
	public float[] getReals() {
		return reals;
	}
	public void setReals(float[] reals) {
		this.reals = reals;
	}
	public String getAllColumns() {
		return allColumns;
	}
	public void setAllColumns(String allColumns) {
		this.allColumns = allColumns;
	}
	public byte [][] getHexStringAsByte () {
		return getHexToByteArray (bytes);
	}
	public byte [][] getClobAsByte () {
		return getByteArray (clobs);
	}
	public byte [][] getCharAsByte () {
		return getByteArray (chars);
	}
	public byte [][] getVarCharAsByte () {
		return getByteArray (varchars);
	}
	public long[] getPstTime() {
		return pstTime;
	}
	public void setPstTime(long[] pstTime) {
		this.pstTime = pstTime;
	}
	public long[] getEstTime() {
		return estTime;
	}
	public void setEstTime(long[] estTime) {
		this.estTime = estTime;
	}
	public long[] getGmtTime() {
		return gmtTime;
	}
	public void setGmtTime(long[] gmtTime) {
		this.gmtTime = gmtTime;
	}
	public long[] getPstDate() {
		return pstDate;
	}
	public void setPstDate(long[] pstDate) {
		this.pstDate = pstDate;
	}
	public long[] getEstDate() {
		return estDate;
	}
	public void setEstDate(long[] estDate) {
		this.estDate = estDate;
	}
	public long[] getGmtDate() {
		return gmtDate;
	}
	public void setGmtDate(long[] gmtDate) {
		this.gmtDate = gmtDate;
	}
	public long[] getPstTimestamp() {
		return pstTimestamp;
	}
	public void setPstTimestamp(long[] pstTimestamp) {
		this.pstTimestamp = pstTimestamp;
	}
	public long[] getEstTimestamp() {
		return estTimestamp;
	}
	public void setEstTimestamp(long[] estTimestamp) {
		this.estTimestamp = estTimestamp;
	}
	public long[] getGmtTimestamp() {
		return gmtTimestamp;
	}
	public void setGmtTimestamp(long[] gmtTimestamp) {
		this.gmtTimestamp = gmtTimestamp;
	}
	
	private byte [][] getByteArray (String [] data) {
		byte [][] byteArr = new byte [data.length][];
		
		for (int i = 0; i < data.length; i++) {
			byteArr [i] = data [i].getBytes(StandardCharsets.UTF_8);
		}
		return 	byteArr;	
    }
	private byte [][] getHexToByteArray (String [] data){
		byte [][] byteArr = new byte [data.length][];
		
		for (int i = 0; i < data.length; i++) {
			byteArr [i] = hexStringToByteArray(data [i]);
		}
		return 	byteArr;
	}
	private static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }
}
