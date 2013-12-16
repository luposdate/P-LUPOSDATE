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
package lupos.cloud.bloomfilter;

/**
 * Für jedes Tripel-Muster wird ein "CloudBitvector" erzeugt in dem die
 * Informationen gespeichert werden die für das Laden des Bitvektors aus HBase
 * später wichtig sind.
 */
public class CloudBitvector {

	/** The row. */
	String row;

	/** The column family. */
	byte[] columnFamily;

	/** The tablename. */
	private String tablename;

	/** The pattern id. */
	Integer patternId = null;

	/** The set id. */
	Integer setId = null;

	/**
	 * Instantiates a new cloud bitvector.
	 * 
	 * @param tablename
	 *            the tablename
	 * @param row
	 *            the row
	 * @param columnFamily
	 *            the column family
	 * @param patternId
	 *            the pattern id
	 */
	public CloudBitvector(String tablename, String row, byte[] columnFamily,
			Integer patternId) {
		super();
		this.tablename = tablename;
		this.row = row;
		this.columnFamily = columnFamily;
		this.patternId = patternId;
		setId = 0;
	}

	/**
	 * Gets the column family.
	 * 
	 * @return the column family
	 */
	public byte[] getColumnFamily() {
		return columnFamily;
	}

	/**
	 * Gets the row.
	 * 
	 * @return the row
	 */
	public String getRow() {
		return row;
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
	 * Sets the inc.
	 */
	public void setInc() {
		this.setId++;
	}

	/**
	 * Gets the sets the id.
	 * 
	 * @return the sets the id
	 */
	public Integer getSetId() {
		return setId;
	}

	/**
	 * Gets the pattern id.
	 * 
	 * @return the pattern id
	 */
	public Integer getPatternId() {
		return patternId;
	}
}
