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
package lupos.cloud.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;

import lupos.cloud.bloomfilter.BitvectorManager;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

// In einer späteren Version vll. direkt in  HBase filtern
/**
 * Mit dieser Klasse werden die Tripel direkt in HBase anhand des Bloomfilters
 * gefiltert
 *
 * Anmerkung: Noch nicht vollständig.
 */
@Deprecated
public class BitvectorFilter extends FilterBase {

	/** The byte bit vector1. */
	protected byte[] byteBitVector1 = null;

	/** The byte bit vector2. */
	protected byte[] byteBitVector2 = null;

	/** The bitvector1. */
	BitSet bitvector1 = null;

	/** The bitvector2. */
	BitSet bitvector2 = null;

	/**
	 * Instantiates a new bitvector filter.
	 */
	public BitvectorFilter() {
		super();
	}

	/**
	 * Instantiates a new bitvector filter.
	 *
	 * @param bitvector1
	 *            the bitvector1
	 */
	public BitvectorFilter(final byte[] bitvector1) {
		this.byteBitVector1 = bitvector1;
		this.bitvector1 = fromByteArray(bitvector1);
	}

	/**
	 * Instantiates a new bitvector filter.
	 *
	 * @param bitvector1
	 *            the bitvector1
	 * @param bitvector2
	 *            the bitvector2
	 */
	public BitvectorFilter(final byte[] bitvector1, final byte[] bitvector2) {
		this.byteBitVector1 = bitvector1;
		this.byteBitVector2 = bitvector1;
		this.bitvector1 = fromByteArray(bitvector1);
		this.bitvector2 = fromByteArray(bitvector2);
	}

	/**
	 * From byte array.
	 *
	 * @param bytes
	 *            the bytes
	 * @return the bit set
	 */
	public static BitSet fromByteArray(final byte[] bytes) {
		return BitSet.valueOf(bytes);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.apache.hadoop.hbase.filter.FilterBase#filterKeyValue(org.apache.hadoop
	 * .hbase.KeyValue)
	 */
	@Override
	public ReturnCode filterKeyValue(final KeyValue kv) {
		final String toSplit = Bytes.toString(kv.getKey());
		if (toSplit.contains(",")) {
			// 1
			final String toAdd1 = toSplit.substring(0, toSplit.indexOf(","));
			if (this.bitvector1 != null
					&& !this.isElementPartOfBitvector(toAdd1, this.bitvector1)) {
				return ReturnCode.NEXT_COL;
			}
			// 2
			final String toAdd2 = toSplit.substring(toSplit.indexOf(",") + 1,
					toSplit.length());

			if (this.bitvector2 != null
					&& !this.isElementPartOfBitvector(toAdd2, this.bitvector2)) {
				return ReturnCode.NEXT_COL;
			}
		} else {
			final String toAdd = Bytes.toString(kv.getKey());
			if (this.bitvector1 != null
					&& !this.isElementPartOfBitvector(toAdd, this.bitvector1)) {
				return ReturnCode.NEXT_COL;
			}
		}
		return ReturnCode.INCLUDE;
	}

	/**
	 * Checks if is element part of bitvector.
	 *
	 * @param element
	 *            the element
	 * @param bitvector
	 *            the bitvector
	 * @return true, if is element part of bitvector
	 */
	private boolean isElementPartOfBitvector(final String element, final BitSet bitvector) {
		final Integer position = BitvectorManager.hash(element.getBytes());
		if (bitvector.get(position)) {
			return true;
		} else {
			return false;
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(final DataOutput out) throws IOException {
		Bytes.writeByteArray(out, this.byteBitVector1);
	}

	//
	/*
	 * (non-Javadoc)
	 *
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(final DataInput in) throws IOException {
		this.byteBitVector1 = Bytes.readByteArray(in);
		this.byteBitVector2 = Bytes.readByteArray(in);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.apache.hadoop.hbase.filter.FilterBase#toString()
	 */
	@Override
	public String toString() {
		return this.getClass().getSimpleName();
	}
}
