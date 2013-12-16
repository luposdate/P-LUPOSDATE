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
package lupos.cloud.pig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import lupos.cloud.bloomfilter.CloudBitvector;
import lupos.cloud.pig.operator.IPigOperator;
import lupos.cloud.pig.operator.PigDistinctOperator;
import lupos.cloud.pig.operator.PigFilterExectuer;
import lupos.cloud.pig.operator.PigFilterOperator;
import lupos.cloud.pig.operator.PigLimitOperator;
import lupos.cloud.pig.operator.PigOrderByOperator;
import lupos.cloud.pig.operator.PigProjectionOperator;
import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.index.Root;
import lupos.engine.operators.singleinput.AddBinding;
import lupos.engine.operators.singleinput.Projection;
import lupos.engine.operators.singleinput.Result;
import lupos.engine.operators.singleinput.filter.Filter;
import lupos.engine.operators.singleinput.modifiers.Limit;
import lupos.engine.operators.singleinput.modifiers.distinct.Distinct;
import lupos.engine.operators.singleinput.sort.Sort;

/**
 * Innerhalb dieser Klasse werden alle Informationen eines PigLatin-Programms
 * verwaltet.
 */
public class PigQuery {

	/** Liste der SinglePigQueries. */
	ArrayList<SinglePigQuery> singleQueries = new ArrayList<SinglePigQuery>();

	/** In dieser Liste werden die Zwischenergebnisse verwaltet. */
	ArrayList<BagInformation> intermediateBags = new ArrayList<BagInformation>();

	/** Das PigLatin-Programm . */
	StringBuilder pigLatin = new StringBuilder();

	/** Gibt Debug Infomrationen aus, wenn true. */
	public static boolean debug = true;

	/** Bindings. */
	private HashMap<String, String> addBinding = new HashMap<String, String>();

	/**
	 * Ersetzt für den den letzten Bag-Alias durch X.
	 */
	public void finishQuery() {
		StringBuilder modifiedPigQuery = new StringBuilder();
		modifiedPigQuery.append(this.pigLatin.toString().replace(
				this.getFinalAlias(), "X"));
		this.pigLatin = modifiedPigQuery;
	}

	/**
	 * Gibt das PigLatin-Programm zurück.
	 * 
	 * @return the pig latin
	 */
	public String getPigLatin() {
		return pigLatin.toString();
	}

	/**
	 * Gibt die Variablenreihenfolge zurück.
	 * 
	 * @return the variable list
	 */
	public ArrayList<String> getVariableList() {
		ArrayList<String> result = new ArrayList<String>();
		for (String elem : intermediateBags.get(0).getBagElements()) {
			// Bindings werden für die Ergebnismenge wieder rückgängig gemacht
			boolean replaced = false;
			for (String oldVar : this.addBinding.keySet()) {
				if (elem.equals(this.addBinding.get(oldVar))) {
					result.add(oldVar.replace("?", ""));
					replaced = true;
				}

			}
			if (!replaced) {
				result.add(elem.replace("?", ""));
			}

		}
		return result;
	}

	/**
	 * Übersetzt einen IndexScanOperator + Folgeoperationen in ein
	 * PigLatin-Programm.
	 * 
	 * @param singlePigQuery
	 *            the single pig query
	 */
	public void addAndPrceedSinglePigQuery(SinglePigQuery singlePigQuery) {
		singlePigQuery.finishQuery();
		this.singleQueries.add(singlePigQuery);
		if (singlePigQuery.getAddBindings() != null) {
			this.addBinding.putAll(singlePigQuery.getAddBindings());
		}
		intermediateBags.add(singlePigQuery.getIntermediateJoins().get(0));
		pigLatin.append(singlePigQuery.getPigLatin());
	}

	/**
	 * Entfernt Zwischenergebnis.
	 * 
	 * @param toRemove
	 *            the to remove
	 */
	public void removeIntermediateBags(BagInformation toRemove) {
		this.intermediateBags.remove(toRemove);
	}

	/**
	 * Adds the intermediate bags.
	 * 
	 * @param newJoin
	 *            the new join
	 */
	public void addIntermediateBags(BagInformation newJoin) {
		this.intermediateBags.add(newJoin);
	}

	/**
	 * Gibt letztes Bag-Element zurück.
	 * 
	 * @return the last added bag
	 */
	public BagInformation getLastAddedBag() {
		BagInformation result = null;
		result = intermediateBags.get(intermediateBags.size() - 1);
		return result;
	}

	/**
	 * Gets the final alias.
	 * 
	 * @return the final alias
	 */
	public String getFinalAlias() {
		return intermediateBags.get(0).getName();
	}

	/**
	 * Übersetzt Operationen in PigLatin-Programm.
	 * 
	 * @param oplist
	 *            the oplist
	 */
	public void addAndExecuteOperation(ArrayList<BasicOperator> oplist) {
		ArrayList<PigFilterOperator> filterOps = new ArrayList<PigFilterOperator>();
		PigProjectionOperator projection = null;
		PigDistinctOperator distinct = null;
		PigLimitOperator limit = null;
		PigOrderByOperator orderBy = null;

		for (BasicOperator op : oplist) {
			if (op instanceof Filter) {
				filterOps.add(new PigFilterOperator((Filter) op));
			} else if (op instanceof Projection) {
				projection = new PigProjectionOperator(
						((Projection) op).getProjectedVariables());
			} else if (op instanceof Distinct) {
				distinct = new PigDistinctOperator();
			} else if (op instanceof Sort) {
				orderBy = new PigOrderByOperator(((Sort) op));
			} else if (op instanceof Limit) {
				limit = new PigLimitOperator(((Limit) op).getLimit());
			} else if (op instanceof Result || op instanceof Root
					|| op instanceof AddBinding) {
				// ignore
			} else {
				throw new RuntimeException(
						"Something is wrong here. Forgot case? Class: "
								+ op.getClass());
			}
		}

		// Filter
		if (filterOps.size() > 0) {
			this.buildAndAppendQuery(new PigFilterExectuer(), filterOps);
		}

		// Limit
		if (limit != null) {
			this.buildAndAppendQuery(limit, filterOps);
		}

		// Order by
		if (orderBy != null) {
			this.buildAndAppendQuery(orderBy, filterOps);
		}

		// Projection
		if (projection != null) {
			if (this.addBinding.size() > 0) {
				projection.replaceVariableInProjection(this.addBinding);
			}
			this.buildAndAppendQuery(projection, filterOps);
		}

		// Distinct
		if (distinct != null) {
			this.buildAndAppendQuery(distinct, filterOps);
		}

	}

	/**
	 * Builds the and append query.
	 * 
	 * @param operator
	 *            the operator
	 * @param filterOps
	 *            the filter ops
	 */
	public void buildAndAppendQuery(IPigOperator operator,
			ArrayList<PigFilterOperator> filterOps) {
		this.pigLatin.append(operator.buildQuery(intermediateBags, debug,
				filterOps));
	}

	/**
	 * Builds the and append query.
	 * 
	 * @param operator
	 *            the operator
	 */
	public void buildAndAppendQuery(IPigOperator operator) {
		this.pigLatin.append(operator.buildQuery(intermediateBags, debug,
				new ArrayList<PigFilterOperator>()));
	}

	/**
	 * Replace variable in projection.
	 * 
	 * @param oldVar
	 *            the old var
	 * @param newVar
	 *            the new var
	 */
	public void replaceVariableInProjection(String oldVar, String newVar) {
		if (this.addBinding == null) {
			this.addBinding = new HashMap<String, String>();
		}
		this.addBinding.put(oldVar, newVar);
	}

	/**
	 * Append.
	 * 
	 * @param toAdd
	 *            the to add
	 */
	public void append(String toAdd) {
		this.pigLatin.append(toAdd);
	}

	/**
	 * Gets the bitvectors.
	 * 
	 * @return the bitvectors
	 */
	public HashMap<String, HashSet<CloudBitvector>> getBitvectors() {
		return this.intermediateBags.get(0).getBitVectors();
	}

	/**
	 * Replace bloomfilter name.
	 * 
	 * @param oldName
	 *            the old name
	 * @param newName
	 *            the new name
	 */
	public void replaceBloomfilterName(String oldName, String newName) {
		String original = this.pigLatin.toString();
		this.pigLatin = new StringBuilder();
		this.pigLatin.append(original.replace(oldName, newName));
	}
}
