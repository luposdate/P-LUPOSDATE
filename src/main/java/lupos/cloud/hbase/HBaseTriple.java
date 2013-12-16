/**
 * Copyright (c) 2013, Institute of Information Systems (Sven Groppe, Thomas Kiencke and contributors of P-LUPOSDATE), University of Luebeck
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * 	- Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 * 	  disclaimer.
 * 	- Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
 * 	  following disclaimer in the documentation and/or other materials provided with the distribution.
 * 	- Neither the name of the University of Luebeck nor the names of its contributors may be used to endorse or promote
 * 	  products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package lupos.cloud.hbase;

/**
 * Speichert die Informationen eines HBase Tripels <=> einer Zeile in HBase.
 */
public class HBaseTriple {

	/** The row_key. */
	String row_key;

	/** The column family. */
	String columnFamily;

	/** The column. */
	String column;

	/** The value. */
	String value;

	/** The tablename. */
	String tablename;

	/**
	 * Instantiates a new h base triple.
	 * 
	 * @param tablename
	 *            the tablename
	 * @param row_key
	 *            the row_key
	 * @param columnFamily
	 *            the column family
	 * @param column
	 *            the column
	 * @param value
	 *            the value
	 */
	public HBaseTriple(String tablename, String row_key, String columnFamily,
			String column, String value) {
		super();
		this.row_key = row_key;
		this.column = column;
		this.columnFamily = columnFamily;
		this.value = value;
		this.tablename = tablename;
	}

	/**
	 * Gets the tablename.
	 * 
	 * @return the tablename
	 */
	public String getTablename() {
		return tablename;
	}

	/**
	 * Gets the row_key.
	 * 
	 * @return the row_key
	 */
	public String getRow_key() {
		return row_key;
	}

	/**
	 * Sets the row_key.
	 * 
	 * @param row_key
	 *            the new row_key
	 */
	public void setRow_key(String row_key) {
		this.row_key = row_key;
	}

	/**
	 * Gets the column.
	 * 
	 * @return the column
	 */
	public String getColumn() {
		return column;
	}

	/**
	 * Sets the column.
	 * 
	 * @param column_family
	 *            the new column
	 */
	public void setColumn(String column_family) {
		this.column = column_family;
	}

	/**
	 * Gets the value.
	 * 
	 * @return the value
	 */
	public String getValue() {
		return value;
	}

	/**
	 * Sets the value.
	 * 
	 * @param value
	 *            the new value
	 */
	public void setValue(String value) {
		this.value = value;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "HBaseTriple [row_key=" + row_key + ", column_family=" + column
				+ ", value=" + value + "]";
	}

	/**
	 * Gets the column family.
	 * 
	 * @return the column family
	 */
	public String getColumnFamily() {
		return columnFamily;
	}

	/**
	 * Sets the column family.
	 * 
	 * @param column_family
	 *            the new column family
	 */
	public void setColumnFamily(String column_family) {
		this.column = column_family;
	}

}
