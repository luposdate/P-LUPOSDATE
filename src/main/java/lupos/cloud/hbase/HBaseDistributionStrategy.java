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

import java.util.Collection;
import java.util.TreeMap;

import lupos.datastructures.items.Triple;

/**
 * Oberkalsse für die HBase Verteilungsstrategie der Tripel.
 */
public abstract class HBaseDistributionStrategy {
	
	/** The table strategy. */
	public static int TABLE_STRATEGY = HexaDistributionTableStrategy.STRAGEGY_ID;
	
	/** The instance. */
	private static HBaseDistributionStrategy instance = null;
	
	/**
	 * Gibt alle Tabellennamen zurück.
	 *
	 * @return the table names
	 */
	public abstract String[] getTableNames();
	
	/**
	 * Gibt die Strategie Instanz zurück.
	 *
	 * @return the table instance
	 */
	public static HBaseDistributionStrategy getTableInstance() {
		if (instance == null) {
			switch (TABLE_STRATEGY) {
			case HexaDistributionTableStrategy.STRAGEGY_ID:
				instance = new HexaDistributionTableStrategy();
				break;
			case HexaSubkeyDistributionTableStrategy.STRAGEGY_ID:
				instance = new HexaSubkeyDistributionTableStrategy();
				break;
			default:
				instance = new HexaDistributionTableStrategy();
				break;
			}
		}
		return instance;
	}

	/**
	 * Generiert anhand eines Tripel die verschiedenen Indizes.
	 *
	 * @param triple the triple
	 * @return the collection
	 */
	public abstract Collection<HBaseTriple> generateIndecesTriple(
			final Triple triple);

	/**
	 * Gibt die Reihenfolge der Elemente wieder.
	 *
	 * @param elements the elements
	 * @param triple the triple
	 * @return the input value
	 */
	public abstract TreeMap<Integer, Object> getInputValue(String elements,
			Triple triple);

	/**
	 * Generiert aus einem Tripel ein HBase Tripel.
	 *
	 * @param tablename the tablename
	 * @param row_key the row_key
	 * @param column the column
	 * @param value the value
	 * @return the h base triple
	 */
	public abstract HBaseTriple generateHBaseTriple(final String tablename,
			final String row_key, final String column, final String value);
	
	/**
	 * Gibt den Namen der Spaltenfamilie zurück.
	 *
	 * @return the column family name
	 */
	public abstract String getColumnFamilyName();

}
