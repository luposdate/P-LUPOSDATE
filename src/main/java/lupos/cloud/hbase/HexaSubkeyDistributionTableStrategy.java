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
 * 
 * Anmerkung: Diese Klasse wurde nur zu Testzwecken genutzt könnte aber ggf. als
 * Grundlage für eine spätere zusätzliche Partitionierung der P_SO Tabelle
 * dienen.
 */
public class HexaSubkeyDistributionTableStrategy extends
		HBaseDistributionStrategy {

	/** The Constant STRAGEGY_ID. */
	public static final int STRAGEGY_ID = 2;

	/** The Constant COLUMN_FAMILY. */
	public static final String COLUMN_FAMILY = "HexaSub";

	private static final int k = 3;

	/*
	 * (non-Javadoc)
	 * 
	 * @see lupos.cloud.hbase.HBaseDistributionStrategy#getTableNames()
	 */
	public String[] getTableNames() {
		String[] result = { "S_PO", "P_SO", "O_SP", "SP_O", "SO_P", "PO_S" };
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lupos.cloud.hbase.HBaseDistributionStrategy#generateIndecesTriple(lupos
	 * .datastructures.items.Triple)
	 */
	public Collection<HBaseTriple> generateIndecesTriple(final Triple triple) {
		ArrayList<HBaseTriple> result = new ArrayList<HBaseTriple>();
		for (String tablename : getTableNames()) {
			String row_key_string = tablename.substring(0,
					tablename.indexOf("_"));
			String column_name_string = tablename.substring(
					tablename.indexOf("_") + 1, tablename.length());

			String row_key = "";
			boolean first = true;
			for (Integer key : getInputValue(row_key_string, triple).keySet()) {
				if (first) {
					first = false;
				} else {
					row_key += ",";
				}
				row_key += getInputValue(row_key_string, triple).get(key);
			}

			String column = "";
			first = true;

			ArrayList<Integer> hashValues = new ArrayList<Integer>();

			for (Integer key : getInputValue(column_name_string, triple)
					.keySet()) {
				if (first) {
					first = false;
				} else {
					column += ",";
				}
				int hashValue = getInputValue(column_name_string, triple).get(
						key).hashCode()
						% k;
				if (hashValue < 0) {
					hashValue = hashValue * (-1);
				}
				hashValues.add(hashValue);
				column += getInputValue(column_name_string, triple).get(key);
			}

			for (Integer hashVal : hashValues) {
				row_key += hashVal;
			}
			result.add(generateHBaseTriple(tablename, row_key, column, ""));
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
	public TreeMap<Integer, Object> getInputValue(String elements, Triple triple) {
		TreeMap<Integer, Object> tm = new TreeMap<Integer, Object>();
		int subject = elements.indexOf('S');
		if (subject > -1)
			tm.put(subject, triple.getSubject());
		int predicate = elements.indexOf('P');
		if (predicate > -1)
			tm.put(predicate, triple.getPredicate());
		int object = elements.indexOf('O');
		if (object > -1)
			tm.put(object, triple.getObject());
		return tm;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lupos.cloud.hbase.HBaseDistributionStrategy#generateHBaseTriple(java.
	 * lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
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
