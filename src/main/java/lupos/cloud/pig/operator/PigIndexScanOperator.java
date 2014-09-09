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
package lupos.cloud.pig.operator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import lupos.cloud.bloomfilter.BitvectorManager;
import lupos.cloud.bloomfilter.CloudBitvector;
import lupos.cloud.hbase.HBaseDistributionStrategy;
import lupos.cloud.pig.BagInformation;
import lupos.cloud.storage.util.CloudManagement;
import lupos.datastructures.items.Variable;
import lupos.engine.operators.tripleoperator.TriplePattern;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * Überführt Luposdate IndexScan-Operator in PigLatin-Programm.
 */
public class PigIndexScanOperator implements IPigOperator {

	/** Zwischenergebnisse. */
	ArrayList<BagInformation> intermediateJoins = null;

	/** Alle Tripel-Muster. */
	Collection<TriplePattern> triplePatternCollection = null;

	/** Counter. */
	int tripleCounter = 0;

	/** Debug Ausgabe. */
	boolean debug = false;

	/**
	 * Instantiates a new pig index scan operator.
	 *
	 * @param tp
	 *            the tp
	 */
	public PigIndexScanOperator(final Collection<TriplePattern> tp) {
		this.triplePatternCollection = tp;
	}

	/**
	 * Mit dieser Methode wird das PigLatin-Programm langsam aufgebaut indem die
	 * einzelnen Tripel-Muster hinzugefügt werden.
	 *
	 * @param intermediateBags
	 *            the intermediate bags
	 * @param debug
	 *            the debug
	 * @param filterOps
	 *            the filter ops
	 * @return the string
	 */
	public String buildQuery(final ArrayList<BagInformation> intermediateBags,
			final boolean debug, final ArrayList<PigFilterOperator> filterOps) {
		this.intermediateJoins = intermediateBags;
		this.debug = debug;
		final StringBuilder result = new StringBuilder();
		for (final TriplePattern triplePattern : this.triplePatternCollection) {
			final BagInformation curPattern = this.getHBaseTable(triplePattern);

			if (debug) {
				result.append("-- TriplePattern: " + triplePattern.toN3String()
						+ "\n");
			}
			/**
			 * Für Triplepattern ?s ?p ?o wird eine beliebige Tabelle komplett
			 * geladen und alle Informationen zuürck gegeben.
			 */
			if (curPattern.allElementsAreVariables()) {
				result.append("PATTERN_"
						+ curPattern.getPatternId()
						+ " = "
						+ "load 'hbase://"
						+ curPattern.getTablename()
						+ "' "
						+ "using lupos.cloud.pig.udfs.HBaseLoadUDF('"
						+ HBaseDistributionStrategy.getTableInstance()
								.getColumnFamilyName() + "', '-loadKey true'");
				if (CloudManagement.bloomfilter_active) {
					result.append(", '', "
							+ " '"
							+ DigestUtils.sha512Hex(
									curPattern.getBagElements().get(1)
											+ curPattern.getPatternId())
									.toString()
							+ "', '"
							+ DigestUtils.sha512Hex(
									curPattern.getBagElements().get(2)
											+ curPattern.getPatternId())
									.toString() + "'");
				}

				result.append(") as (rowkey:chararray, columncontent_"
						+ this.tripleCounter + ":map[]");
				if (CloudManagement.bloomfilter_active) {
					result.append(", bloomfilter1:bytearray, bloomfilter2:bytearray");
				}
				result.append(");\n");
				result.append(curPattern.getName()
						+ " = foreach "
						+ "PATTERN_"
						+ curPattern.getPatternId()
						+ " generate $0, flatten(lupos.cloud.pig.udfs.MapToBagUDF($1");
				if (CloudManagement.bloomfilter_active) {
					result.append(", $2, $3");
				}
				result.append("));\n");
			} else if (curPattern.allElementsAreLiterals()) {
				// do nothing, todo
				return "";
			} else {
				result.append(
				/**
				 * Für alle anderen Triplepattern wird in den jeweiligen
				 * Tabellen gesucht und nur das Ergebniss (der Spaltenname)
				 * zurückgegeben.
				 *
				 * Anmerkung bzgl Bitvektoren: Statt den Pfad des jeweiligen
				 * Bitvektor wird ein Platzhalter (SHA 512 HEX Wert) eingesetzt.
				 * Dieser Platzhalter wird später bei der Bitvektorberechnung
				 * durch den tatsächlichen Pfad ersetzt. Der Hash wird
				 * folgendermaßen berechnet:
				 *
				 * h(x) =
				 */
				"PATTERN_"
						+ curPattern.getPatternId()
						+ " = "
						+ "load 'hbase://"
						+ curPattern.getTablename()
						+ "' "
						+ "using lupos.cloud.pig.udfs.HBaseLoadUDF('"
						+ HBaseDistributionStrategy.getTableInstance()
								.getColumnFamilyName() + "', '', '"
						+ curPattern.getLiterals() + "'");
				if (CloudManagement.bloomfilter_active) {
					result.append(((curPattern.getBagElements().size() == 1) ? ", '"
							+ DigestUtils.sha512Hex(
									curPattern.getBagElements().get(0)
											+ curPattern.getPatternId())
									.toString() + "'" : ", '"
							+ DigestUtils.sha512Hex(
									curPattern.getBagElements().get(0)
											+ curPattern.getPatternId())
									.toString()
							+ "', '"
							+ DigestUtils.sha512Hex(
									curPattern.getBagElements().get(1)
											+ curPattern.getPatternId())
									.toString() + "'"));
				}

				result.append(") as (columncontent_" + this.tripleCounter + ":map[]");
				if (CloudManagement.bloomfilter_active) {
					result.append(((curPattern.getBagElements().size() == 1) ? ", bloomfilter1:bytearray"
							: ", bloomfilter1:bytearray, bloomfilter2:bytearray"));
				}
				result.append(");\n");

				result.append(curPattern.getName() + " = foreach PATTERN_"
						+ curPattern.getPatternId()
						+ " generate flatten(lupos.cloud.pig.udfs.MapToBagUDF("
						+ "$0");
				if (CloudManagement.bloomfilter_active) {
					result.append(((curPattern.getBagElements().size() == 1) ? ", $1"
							: ", $1, $2"));
				}

				result.append(")) as "
						+ ((curPattern.getBagElements().size() == 1) ? "(output"
								+ this.tripleCounter + ":chararray);"
								: "(output1_" + this.tripleCounter
										+ ":chararray, output2_"
										+ this.tripleCounter + ":chararray); ")
						+ "\n");
			}
			this.intermediateJoins.add(curPattern);

			// add bitvector
			if ((curPattern.getBagElements().size() == 1)) {
				curPattern.addBitvector(curPattern.getBagElements().get(0),
						new CloudBitvector(curPattern.getTablename(),
								curPattern.getLiterals(),
								BitvectorManager.bloomfilter1ColumnFamily,
								curPattern.getPatternId()));
			} else if ((curPattern.getBagElements().size() == 2)) {
				curPattern.addBitvector(curPattern.getBagElements().get(0),
						new CloudBitvector(curPattern.getTablename(),
								curPattern.getLiterals(),
								BitvectorManager.bloomfilter1ColumnFamily,
								curPattern.getPatternId()));
				curPattern.addBitvector(curPattern.getBagElements().get(1),
						new CloudBitvector(curPattern.getTablename(),
								curPattern.getLiterals(),
								BitvectorManager.bloomfilter2ColumnFamily,
								curPattern.getPatternId()));
			} else if ((curPattern.getBagElements().size() == 3)) {
				curPattern.addBitvector(
						curPattern.getBagElements().get(0),
						new CloudBitvector(curPattern.getTablename(),
								curPattern.getLiterals(), null, curPattern
										.getPatternId()));
				curPattern.addBitvector(
						curPattern.getBagElements().get(1),
						new CloudBitvector(curPattern.getTablename(),
								curPattern.getLiterals(), null, curPattern
										.getPatternId()));
				curPattern.addBitvector(
						curPattern.getBagElements().get(2),
						new CloudBitvector(curPattern.getTablename(),
								curPattern.getLiterals(), null, curPattern
										.getPatternId()));
			}

			if (debug) {
				result.append("\n");
			}
			this.tripleCounter++;
		}
		return result.toString();
	}

	/**
	 * Gibt für ein Tripel-Muster die korrespondierende HBase Tabelle zurück.
	 *
	 * @param triplePattern
	 *            the triple pattern
	 * @return the h base table
	 */
	private BagInformation getHBaseTable(final TriplePattern triplePattern) {
		final int subject = triplePattern.getSubject().getClass() == Variable.class ? 1
				: 0;
		final int predicate = triplePattern.getPredicate().getClass() == Variable.class ? 10
				: 0;
		final int object = triplePattern.getObject().getClass() == Variable.class ? 100
				: 0;

		BagInformation result = null;
		switch (subject + predicate + object) {
		case 110:
			result = new BagInformation(triplePattern, "s_po",
					"INTERMEDIATE_BAG_");
			break;
		case 101:
			result = new BagInformation(triplePattern, "p_so",
					"INTERMEDIATE_BAG_");
			break;
		case 11:
			result = new BagInformation(triplePattern, "o_sp",
					"INTERMEDIATE_BAG_");
			break;
		case 100:
			result = new BagInformation(triplePattern, "sp_o",
					"INTERMEDIATE_BAG_");
			break;
		case 10:
			result = new BagInformation(triplePattern, "so_p",
					"INTERMEDIATE_BAG_");
			break;
		case 1:
			result = new BagInformation(triplePattern, "po_s",
					"INTERMEDIATE_BAG_");
			break;
		case 111:
			// Wenn alles Variablen sind kann eine beliebige Tabelle verwendet
			// werden, hier wird S_PO genommen
			result = new BagInformation(triplePattern, "s_po",
					"INTERMEDIATE_BAG_");
			break;
		case 0:
			// Wenn alles Literale sind kann eine beliebige Tabelle verwendet
			// werden, hier wird SO_P genommen
			result = new BagInformation(triplePattern, "so_p",
					"INTERMEDIATE_BAG_");
			break;
		default:
			break;
		}

		// for (String item : result.getVariables()) {
		// joinVariables.add(item);
		// }

		return result;
	}

	/**
	 * Multi join over two variables.
	 *
	 * @return the string
	 */
	public String multiJoinOverTwoVariables() {
		final StringBuilder result = new StringBuilder();
		HashSet<String> equalVariables = null;
		final HashSet<BagInformation> toJoin = new HashSet<BagInformation>();
		boolean found = false;

		/*
		 * Es wird die Join-Menge gesucht bei dem eine Variable am häufigsten
		 * vorkommt. Für die Join-Mengen wird dann ein PigLatin Join ausgegeben
		 * und die Join-Mengen werden zu einer vereinigt.
		 */
		for (final BagInformation curJoin1 : this.intermediateJoins) {
			final HashSet<String> variables1 = new HashSet<String>();

			// alle Mengen die weniger als 2 variablen haben sind für diesen
			// Fall nicht interessant
			if (curJoin1.getVariables().size() < 2 || toJoin.contains(curJoin1)) {
				continue;
			}

			// Füge alle Variablen dem Set hinzu
			for (final String var : curJoin1.getVariables()) {
				variables1.add(var);
			}

			// Finde eine Menge die die selben Variablen hat
			for (final BagInformation curJoin2 : this.intermediateJoins) {
				final HashSet<String> variables2 = new HashSet<String>();

				// Die neue Menge darf nicht die selbe seine wie die erste und
				// muss auch mehr als eine Variable haben
				if (curJoin1.equals(curJoin2)
						|| curJoin2.getVariables().size() < 2
						|| toJoin.contains(curJoin2)) {
					continue;
				}

				// Füge alle Variablen dem Set hinzu
				for (final String var : curJoin2.getVariables()) {
					variables2.add(var);
				}

				// Vergleiche Set1 mit Set2 und speicher selbe Variablen ab
				final HashSet<String> tmpEqualVariables = new HashSet<String>();
				for (final String entry1 : variables1) {
					for (final String entry2 : variables2) {
						if (entry1.equals(entry2)) {
							tmpEqualVariables.add(entry1);
						}
					}
				}
				if (tmpEqualVariables.size() > 1) {
					equalVariables = tmpEqualVariables;
					found = true;
					toJoin.add(curJoin1);
					toJoin.add(curJoin2);
				}

			}

		}

		if (!found) {
			return null;
		}
		result.append(this.getPigMultiJoinWith2Columns(
				new ArrayList<BagInformation>(toJoin), new ArrayList<String>(
						equalVariables)));

		for (final BagInformation toRemove : toJoin) {
			this.intermediateJoins.remove(toRemove);
		}
		// this.joinVariables.remove(variableToJoin);
		return result.toString();
	}

	/**
	 * Multi join over one variable.
	 *
	 * @return the string
	 */
	public String multiJoinOverOneVariable() {
		final StringBuilder result = new StringBuilder();
		ArrayList<BagInformation> joinAliases = null;
		final ArrayList<ArrayList<BagInformation>> joinCandidates = new ArrayList<ArrayList<BagInformation>>();
		final ArrayList<String> joinVariablesCandidates = new ArrayList<String>();

		/*
		 * Es wird die Join-Menge gesucht bei dem eine Variable am häufigsten
		 * vorkommt. Für die Join-Mengen wird dann ein PigLatin Join ausgegeben
		 * und die Join-Mengen werden zu einer vereinigt.
		 */
		for (final BagInformation curJoin : this.intermediateJoins) {
			boolean found = false;
			String joinVariable = "";
			joinAliases = new ArrayList<BagInformation>();
			// JoinInformation curJoin = intermediateJoins.get(0);
			joinAliases.add(curJoin);
			for (int i = 0; i < this.intermediateJoins.size(); i++) {
				if (this.intermediateJoins.get(i).equals(curJoin)) {
					continue;
				}
				for (String variable1 : curJoin.getBagElements()) {
					if (found) {
						variable1 = joinVariable;
					}
					for (final String variable2 : this.intermediateJoins.get(i)
							.getBagElements()) {
						if (variable1.equals(variable2)) {
							found = true;
							joinVariable = variable1;
							joinAliases.add(this.intermediateJoins.get(i));
							break;
						}
					}
					if (found) {
						break;
					}
				}
			}

			joinCandidates.add(joinAliases);
			joinVariablesCandidates.add(joinVariable);

		}

		ArrayList<BagInformation> patternToJoin = joinCandidates.get(0);
		String variableToJoin = joinVariablesCandidates.get(0);
		int i = 0;
		for (final ArrayList<BagInformation> curCandidate : joinCandidates) {
			if (curCandidate.size() > patternToJoin.size()) {
				patternToJoin = curCandidate;
				variableToJoin = joinVariablesCandidates.get(i);
			}
			i++;
		}

		result.append(this.getPigMultiJoin(patternToJoin, variableToJoin));

		for (final BagInformation toRemove : patternToJoin) {
			this.intermediateJoins.remove(toRemove);
		}
		// this.joinVariables.remove(variableToJoin);
		return result.toString();
	}

	/**
	 * Joint mehrere Mengen über ein Element (Ausgabe als PigLatin Programm).
	 *
	 * @param joinOverItem
	 *            the join over item
	 * @param joinElement
	 *            the join element
	 * @return the pig multi join
	 */
	public String getPigMultiJoin(final ArrayList<BagInformation> joinOverItem,
			final String joinElement) {
		final StringBuilder result = new StringBuilder();

		for (final BagInformation bag : joinOverItem) {
			if (bag.isVariableOptional(joinElement)) {
				throw new RuntimeException(
						"Join over optional variable is not allowed in pig!");
			}
		}

		if (this.debug) {
			result.append("-- Join over " + joinElement.toString() + "\n");
		}

		final BagInformation curJoinInfo = new BagInformation("INTERMEDIATE_BAG_"
				+ BagInformation.idCounter);
		result.append(curJoinInfo.getName() + " = JOIN");
		int i = 0;
		for (final BagInformation curPattern : joinOverItem) {
			i++;
			for (final String s : curPattern.getBagElements()) {
				curJoinInfo.getBagElements().add(s);
			}
			result.append(" " + curPattern.getName() + " BY $"
					+ curPattern.getItemPos(joinElement));
			if (i < joinOverItem.size()) {
				result.append(",");
			} else {
				if (CloudManagement.PARALLEL_REDUCE_OPERATIONS > 1) {
					result.append(" PARALLEL "
							+ CloudManagement.PARALLEL_REDUCE_OPERATIONS);
				}
				result.append(";\n");
			}

			for (final String elem : curPattern.getOptionalJoinElements()) {
				curJoinInfo.addOptionalElements(elem);
			}
			curJoinInfo.addBitVectors(curPattern.getBitVectors());
		}
		curJoinInfo.setPatternId(BagInformation.idCounter);
		curJoinInfo.addAppliedFilters(BagInformation
				.mergeAppliedFilters(joinOverItem));

		result.append(this.removeDuplicatedAliases(curJoinInfo));
		this.intermediateJoins.add(curJoinInfo);
		BagInformation.idCounter++;

		return result.toString();
	}

	/**
	 * Gets the pig multi join with2 columns.
	 *
	 * @param joinOverItem
	 *            the join over item
	 * @param joinElements
	 *            the join elements
	 * @return the pig multi join with2 columns
	 */
	public String getPigMultiJoinWith2Columns(
			final ArrayList<BagInformation> joinOverItem,
			final ArrayList<String> joinElements) {
		final StringBuilder result = new StringBuilder();

		for (final String var : joinElements) {
			for (final BagInformation bag : joinOverItem) {
				if (bag.isVariableOptional(var)) {
					throw new RuntimeException(
							"Join over optional variable is not allowed in pig!");
				}
			}
		}

		if (this.debug) {
			result.append("-- Join over " + joinElements.toString() + "\n");
		}

		final BagInformation curJoinInfo = new BagInformation("INTERMEDIATE_BAG_"
				+ BagInformation.idCounter);

		result.append(curJoinInfo.getName() + " = JOIN");
		int i = 0;
		for (final BagInformation curPattern : joinOverItem) {
			i++;
			for (final String s : curPattern.getBagElements()) {
				curJoinInfo.getBagElements().add(s);
			}
			result.append(" " + curPattern.getName() + " BY ($"
					+ curPattern.getItemPos(joinElements.get(0)) + ",$"
					+ curPattern.getItemPos(joinElements.get(1)) + ")");
			if (i < joinOverItem.size()) {
				result.append(",");
			} else {
				result.append(";\n");
			}

			for (final String elem : curPattern.getOptionalJoinElements()) {
				curJoinInfo.addOptionalElements(elem);
			}

			curJoinInfo.addBitVectors(curPattern.getBitVectors());

		}
		curJoinInfo.setPatternId(BagInformation.idCounter);
		curJoinInfo.addAppliedFilters(BagInformation
				.mergeAppliedFilters(joinOverItem));

		result.append(this.removeDuplicatedAliases(curJoinInfo));
		this.intermediateJoins.add(curJoinInfo);
		BagInformation.idCounter++;

		return result.toString();
	}

	/**
	 * Gets the final alias.
	 *
	 * @return the final alias
	 */
	public String getFinalAlias() {
		return this.intermediateJoins.get(0).getName();
	}

	// hat keinen Vorteil gebracht
	/**
	 * Removes the duplicated aliases.
	 *
	 * @param oldJoin
	 *            the old join
	 * @return the string
	 */
	@Deprecated
	public String removeDuplicatedAliases(final BagInformation oldJoin) {
		return "";
		// StringBuilder result = new StringBuilder();
		// // prüfe ob es doppelte Aliases gibt und entferne diese
		// ArrayList<String> newElements = new ArrayList<String>();
		// boolean foundDuplicate = false;
		//
		// for (String elem : oldJoin.getJoinElements()) {
		// if (newElements.contains(elem)
		// // Sonderfall z.B. ?author und ?author2 überpruefen
		// && elem.equals(newElements.get(newElements.indexOf(elem)))) {
		// foundDuplicate = true;
		// } else {
		// newElements.add(elem);
		// }
		// }
		//
		// System.out.println("V: " + oldJoin.getJoinElements());
		// System.out.println("N: " + newElements);
		// if (foundDuplicate) {
		// result.append(oldJoin.getName() + " = FOREACH " + oldJoin.getName()
		// + " GENERATE ");
		// boolean first = true;
		// for (String elem : newElements) {
		// if (!first) {
		// result.append(", ");
		// }
		// result.append("$" + oldJoin.getItemPos(elem));
		// first = false;
		// }
		// result.append(";\n");
		// oldJoin.setJoinElements(newElements);
		// }
		//
		// return result.toString();

	}
}
