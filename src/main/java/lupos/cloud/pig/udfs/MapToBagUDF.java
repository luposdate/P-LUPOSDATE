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
package lupos.cloud.pig.udfs;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import lupos.cloud.bloomfilter.BitvectorManager;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.pig.EvalFunc;
import org.apache.pig.OrderedLoadFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * UDF Funktion für Pig. In dieser Klasse wird die Map-Datenstruktur in eine
 * "Bag" überführt um diese dann weiter zu verarbeiten. Des Weiteren werden die
 * Elemente an dieser Stelle getrennt und der Bloomfilter angewandt.
 */
public class MapToBagUDF extends EvalFunc<DataBag> implements OrderedLoadFunc {

	/** The Constant bagFactory. */
	private static final BagFactory bagFactory = BagFactory.getInstance();
	private static final TupleFactory tupleFactory = TupleFactory.getInstance();

	private BitSet bitvector1 = null;
	private BitSet bitvector2 = null;

	/*
	 * (non-Javadoc)
	 *
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public DataBag exec(final Tuple input) throws IOException {
		final DataBag result = bagFactory.newDefaultBag();
		try {
			final Map<String, DataByteArray> cfMap = (HashMap<String, DataByteArray>) input
					.get(0);

			if (input.size() == 2) {
				final Object b1 = input.get(1);
				if (b1 != null) {
					this.bitvector1 = (BitSet) b1;
				}
			}

			if (input.size() == 3) {
				final Object b1 = input.get(1);
				if (b1 != null) {
					this.bitvector1 = (BitSet) b1;
				}
				final Object b2 = input.get(2);
				if (b2 != null) {
					this.bitvector2 = (BitSet) b2;
				}
			}

			if (cfMap != null) {
				for (final String quantifier : cfMap.keySet()) {
					final String[] columnname = quantifier.split(",");

					if (columnname.length > 1) {
						if (this.bitvector1 != null
								&& !this.isElementPartOfBitvector(
										columnname[0].getBytes(), this.bitvector1)) {
							continue;
						}

						// 2
						if (this.bitvector2 != null
								&& !this.isElementPartOfBitvector(
										columnname[1].getBytes(), this.bitvector2)) {
							continue;
						}

					} else {
						if (this.bitvector1 != null
								&& !this.isElementPartOfBitvector(
										columnname[0].getBytes(), this.bitvector1)) {
							continue;
						}
					}

					if (columnname.length > 1) {
						final Tuple toAdd = tupleFactory.newTuple(2);
						toAdd.set(0, columnname[0]);
						toAdd.set(1, columnname[1]);
						result.add(toAdd);
					} else {
						final Tuple toAdd = tupleFactory.newTuple(1);
						toAdd.set(0, columnname[0]);
						result.add(toAdd);
					}
				}
			}

		} catch (final Exception e) {
			throw new RuntimeException("MapToBag error", e);
		}
		return result;
	}

	@Override
	public WritableComparable<?> getSplitComparable(final InputSplit split)
			throws IOException {
		return null;
	}

	private boolean isElementPartOfBitvector(final byte[] element, final BitSet bitvector) {
		final Integer position = BitvectorManager.hash(element);
		if (bitvector.get(position)) {
			return true;
		} else {
			return false;
		}
	}
}
