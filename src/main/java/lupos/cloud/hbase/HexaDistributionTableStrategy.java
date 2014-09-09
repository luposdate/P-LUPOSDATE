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

import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;

import lupos.datastructures.items.Triple;

/**
 * Die konkrete Implementierung einer Verteilungsstrategie. Bei dieser Strategie
 * wird jedes Tripel nach 6 unterschiedlichen Indizierungsschlüsseln in Tabellen
 * eingeordnet. Dabei ist der Schlüssel jeweils der rowKey und der Wert wird als
 * Spaltenname gespeichert. Der eigentliceh Zellenwert in der HBase Tabelle
 * bleibt leer.
 */
public class HexaDistributionTableStrategy extends HBaseDistributionStrategy {

	/** The Constant STRAGEGY_ID. */
	public static final int STRAGEGY_ID = 1;

	/** The Constant COLUMN_FAMILY. */
	public static final String COLUMN_FAMILY = "Hexa";

	/*
	 * (non-Javadoc)
	 *
	 * @see lupos.cloud.hbase.HBaseDistributionStrategy#getTableNames()
	 */
	@Override
	public String[] getTableNames() {
		final String[] result = { "s_po", "p_so", "o_sp", "sp_o", "so_p", "po_s" };
		return result;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * lupos.cloud.hbase.HBaseDistributionStrategy#generateIndecesTriple(lupos
	 * .datastructures.items.Triple)
	 */
	@Override
	public Collection<HBaseTriple> generateIndecesTriple(final Triple triple) {
		final ArrayList<HBaseTriple> result = new ArrayList<HBaseTriple>();
		for (final String tablename : this.getTableNames()) {
			final String row_key_string = tablename.substring(0,
					tablename.indexOf("_"));
			final String column_name_string = tablename.substring(
					tablename.indexOf("_") + 1, tablename.length());

			String row_key = "";
			boolean first = true;
			for (final Integer key : this.getInputValue(row_key_string, triple).keySet()) {
				if (first) {
					first = false;
				} else {
					row_key += ",";
				}
				row_key += this.getInputValue(row_key_string, triple).get(key);
			}

			String column = "";
			first = true;
			for (final Integer key : this.getInputValue(column_name_string, triple)
					.keySet()) {
				if (first) {
					first = false;
				} else {
					column += ",";
				}
				column += this.getInputValue(column_name_string, triple).get(key);
			}
			result.add(this.generateHBaseTriple(tablename, row_key, column, ""));
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * lupos.cloud.hbase.HBaseDistributionStrategy#getInputValue(java.lang.String
	 * , lupos.datastructures.items.Triple)
	 */
	@Override
	public TreeMap<Integer, Object> getInputValue(final String elements, final Triple triple) {
		final TreeMap<Integer, Object> tm = new TreeMap<Integer, Object>();
		final int subject = elements.indexOf('s');
		if (subject > -1) {
			tm.put(subject, triple.getSubject());
		}
		final int predicate = elements.indexOf('p');
		if (predicate > -1) {
			tm.put(predicate, triple.getPredicate());
		}
		final int object = elements.indexOf('o');
		if (object > -1) {
			tm.put(object, triple.getObject());
		}
		return tm;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * lupos.cloud.hbase.HBaseDistributionStrategy#generateHBaseTriple(java.
	 * lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public HBaseTriple generateHBaseTriple(final String tablename,
			final String row_key, final String column, final String value) {
		return new HBaseTriple(tablename, row_key, COLUMN_FAMILY, column, value);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see lupos.cloud.hbase.HBaseDistributionStrategy#getColumnFamilyName()
	 */
	@Override
	public String getColumnFamilyName() {
		return COLUMN_FAMILY;
	}
}
