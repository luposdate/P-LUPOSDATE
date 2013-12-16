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
import lupos.cloud.pig.operator.PigFilterOperator;
import lupos.datastructures.items.Item;
import lupos.engine.operators.tripleoperator.TriplePattern;

/**
 * Für jede PigLatin Bag-Datenstruktur wird ein BagInformation-Objekt erzeugt.
 * 
 */
public class BagInformation {

	/**  id counter. */
	public static Integer idCounter = 0;

	/**  pattern id. */
	Integer patternId;

	/** Name der Bag. */
	String name;

	/** Variablennamen der Elemente die sich in dieser Bag befinden.. */
	ArrayList<String> bagElements = new ArrayList<String>();

	/** Variablen die sich aus Optionals ergeben haben (ggf. ungebunden). */
	HashSet<String> optionalBagElements = new HashSet<String>();

	/** Tripel-Muster. */
	TriplePattern triplePattern;

	/** The applied filters. */
	ArrayList<PigFilterOperator> appliedFilters = new ArrayList<PigFilterOperator>();

	/** Tabellenname. */
	private String tablename;

	/** The bit vectors. */
	private HashMap<String, HashSet<CloudBitvector>> bitVectors = new HashMap<String, HashSet<CloudBitvector>>();

	/**
	 * Instantiates a new bag information.
	 */
	public BagInformation() {
		this.name = "INTERMEDIATE_BAG_" + BagInformation.idCounter;
		this.setPatternId(idCounter);
		idCounter++;
	}

	/**
	 * Instantiates a new bag information.
	 *
	 * @param triplePattern the triple pattern
	 * @param tablename the tablename
	 * @param name the name
	 */
	public BagInformation(TriplePattern triplePattern, String tablename,
			String name) {
		super();
		this.triplePattern = triplePattern;
		this.patternId = idCounter;
		this.name = name + this.patternId;
		idCounter++;
		this.tablename = tablename;
		for (Item item : triplePattern.getItems()) {
			if (item.isVariable()) {
				bagElements.add(item.toString());
			}
		}
	}

	/**
	 * Instantiates a new bag information.
	 * 
	 * @param name
	 *            the name
	 */
	public BagInformation(String name) {
		this.name = name;
	}

	/**
	 * Gets the pattern id.
	 * 
	 * @return the pattern id
	 */
	public Integer getPatternId() {
		return patternId;
	}

	/**
	 * Sets the pattern id.
	 * 
	 * @param patternId
	 *            the new pattern id
	 */
	public void setPatternId(Integer patternId) {
		this.patternId = patternId;
	}

	/**
	 * Gets the name.
	 * 
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Sets the name.
	 * 
	 * @param name
	 *            the new name
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Gets the join elements.
	 * 
	 * @return the join elements
	 */
	public ArrayList<String> getBagElements() {
		return bagElements;
	}

	/**
	 * Sets the bag elements.
	 * 
	 * @param joinElements
	 *            the new join elements
	 */
	public void setJoinElements(ArrayList<String> joinElements) {
		this.bagElements = joinElements;
	}

	/**
	 * Adds the bag elements.
	 *
	 * @param elem the elem
	 */
	public void addBagElements(String elem) {
		this.bagElements.add(elem);
	}

	/**
	 * Adds the optional elements.
	 *
	 * @param elem the elem
	 */
	public void addOptionalElements(String elem) {
		this.optionalBagElements.add(elem);
	}

	/**
	 * Gets the optional join elements.
	 *
	 * @return the optional join elements
	 */
	public HashSet<String> getOptionalJoinElements() {
		return optionalBagElements;
	}

	/**
	 * Gets the literals.
	 * 
	 * @return the literals
	 */
	public String getLiterals() {
		StringBuilder result = new StringBuilder();
		boolean first = true;
		for (Item item : triplePattern.getItems()) {
			if (!item.isVariable()) {
				result.append(first ? item.toString() : "," + item.toString());
				first = false;
			}
		}
		return result.toString();
	}

	/**
	 * Checks if is variable optional.
	 *
	 * @param var the var
	 * @return true, if is variable optional
	 */
	public boolean isVariableOptional(String var) {
		return optionalBagElements.contains(var);
	}

	/**
	 * Gets the variables.
	 * 
	 * @return the variables
	 */
	public ArrayList<String> getVariables() {
		ArrayList<String> result = new ArrayList<String>();
		if (triplePattern != null) {
			for (Item item : triplePattern.getItems()) {
				if (item.isVariable()) {
					result.add(item.toString());
				}
			}
		}
		return result;
	}

	/**
	 * Gets the item pos.
	 * 
	 * @param itemID
	 *            the item id
	 * @return the item pos
	 */
	public Integer getItemPos(String itemID) {
		return this.bagElements.indexOf(itemID);
	}

	/**
	 * All elements are variables.
	 * 
	 * @return true, if successful
	 */
	public boolean allElementsAreVariables() {
		for (Item item : triplePattern.getItems()) {
			if (!item.isVariable()) {
				return false;
			}
		}
		return true;
	}

	/**
	 * All elements are literals.
	 * 
	 * @return true, if successful
	 */
	public boolean allElementsAreLiterals() {
		for (Item item : triplePattern.getItems()) {
			if (item.isVariable()) {
				return false;
			}
		}
		return true;
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
	 * Adds the applied filters.
	 *
	 * @param appliedFilter the applied filter
	 */
	public void addAppliedFilters(PigFilterOperator appliedFilter) {
		this.appliedFilters.add(appliedFilter);
	}

	/**
	 * Adds the applied filters.
	 *
	 * @param appliedFilters the applied filters
	 */
	public void addAppliedFilters(ArrayList<PigFilterOperator> appliedFilters) {
		for (PigFilterOperator filter : appliedFilters) {
			this.appliedFilters.add(filter);
		}
	}

	/**
	 * Gets the applied filters.
	 *
	 * @return the applied filters
	 */
	public ArrayList<PigFilterOperator> getAppliedFilters() {
		return appliedFilters;
	}

	/**
	 * Filter applied.
	 *
	 * @param appliedFilter the applied filter
	 * @return true, if successful
	 */
	public boolean filterApplied(PigFilterOperator appliedFilter) {
		return this.appliedFilters.contains(appliedFilter);
	}

	/**
	 * Merge applied filters.
	 *
	 * @param joins the joins
	 * @return the array list
	 */
	public static ArrayList<PigFilterOperator> mergeAppliedFilters(
			ArrayList<BagInformation> joins) {
		ArrayList<PigFilterOperator> result = new ArrayList<PigFilterOperator>();
		for (BagInformation j1 : joins) {
			for (PigFilterOperator filter1 : j1.getAppliedFilters()) {
				boolean filterInEveryJoin = true;
				for (BagInformation j2 : joins) {
					if (!j2.getAppliedFilters().contains(filter1)) {
						filterInEveryJoin = false;
					}
				}
				if (filterInEveryJoin) {
					result.add(filter1);
				}

			}
		}
		return result;
	}

	/**
	 * Merge optional variables.
	 *
	 * @param inputBags the input bags
	 */
	public void mergeOptionalVariables(ArrayList<BagInformation> inputBags) {
		for (BagInformation bag : inputBags) {
			this.mergeOptionalVariables(bag);
		}
	}

	/**
	 * Merge optional variables.
	 *
	 * @param bag the bag
	 */
	public void mergeOptionalVariables(BagInformation bag) {
		for (String var : bag.getOptionalJoinElements()) {
			this.optionalBagElements.add(var);
		}
	}

	/**
	 * Adds the bitvector.
	 *
	 * @param var the var
	 * @param vector the vector
	 */
	public void addBitvector(String var, CloudBitvector vector) {
		HashSet<CloudBitvector> list = this.bitVectors.get(var);
		if (list == null) {
			list = new HashSet<CloudBitvector>();
			list.add(vector);
			this.bitVectors.put(var, list);
		} else {
			list.add(vector);
		}
	}

	/**
	 * Gets the bit vectors.
	 *
	 * @return the bit vectors
	 */
	public HashMap<String, HashSet<CloudBitvector>> getBitVectors() {
		return bitVectors;
	}

	/**
	 * Gets the bit vector.
	 *
	 * @param var the var
	 * @return the bit vector
	 */
	public HashSet<CloudBitvector> getBitVector(String var) {
		return bitVectors.get(var);
	}

	/**
	 * Adds the bit vectors.
	 *
	 * @param bitVectors the bit vectors
	 */
	public void addBitVectors(
			HashMap<String, HashSet<CloudBitvector>> bitVectors) {
		for (String key : bitVectors.keySet()) {
			HashSet<CloudBitvector> list = this.bitVectors.get(key);
			if (list == null) {
				list = new HashSet<CloudBitvector>();
				for (CloudBitvector v : bitVectors.get(key)) {
					list.add(v);
				}
				this.bitVectors.put(key, list);

			} else {
				for (CloudBitvector v : bitVectors.get(key)) {
					list.add(v);
				}
				this.bitVectors.put(key, list);
			}
		}
	}

	/**
	 * Adds the bitvector.
	 *
	 * @param var the var
	 * @param bitVectors the bit vectors
	 */
	public void addBitvector(String var, HashSet<CloudBitvector> bitVectors) {
		if (bitVectors == null) {
			return;
		}

		HashSet<CloudBitvector> list = this.bitVectors.get(var);
		if (list == null) {
			list = new HashSet<CloudBitvector>();
			for (CloudBitvector v : bitVectors) {
				list.add(v);
			}
			this.bitVectors.put(var, list);

		} else {
			for (CloudBitvector v : bitVectors) {
				list.add(v);
			}
			this.bitVectors.put(var, list);
		}
	}

	/**
	 * Merge bit vecor.
	 *
	 * @param var the var
	 * @param bitVectors the bit vectors
	 */
	public void mergeBitVecor(String var, HashSet<CloudBitvector> bitVectors) {
		if (bitVectors == null) {
			return;
		}

		HashSet<CloudBitvector> list = this.bitVectors.get(var);
		if (list == null) {
			list = new HashSet<CloudBitvector>();
			for (CloudBitvector v : bitVectors) {
				v.setInc();
				list.add(v);
			}
			this.bitVectors.put(var, list);

		} else {
			for (CloudBitvector v : bitVectors) {
				v.setInc();
				list.add(v);
			}
			this.bitVectors.put(var, list);
		}

	}

}
