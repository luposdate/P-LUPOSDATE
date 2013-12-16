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
package lupos.cloud.optimizations.logical.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import lupos.cloud.operator.CloudSubgraphContainer;
import lupos.cloud.operator.IndexScanContainer;
import lupos.cloud.operator.MultiIndexScanContainer;
import lupos.cloud.operator.ICloudSubgraphExecutor;
import lupos.datastructures.items.Variable;
import lupos.distributed.storage.distributionstrategy.TriplePatternNotSupportedError;
import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.OperatorIDTuple;
import lupos.engine.operators.index.Root;
import lupos.engine.operators.singleinput.Result;
import lupos.optimizations.logical.rules.generated.runtime.Rule;

/**
 * Kapselt den IndexScan- oder MultiIndexScanContainer in einen eigenen
 * Container.
 */
public class AddSubGraphContainerRule extends Rule {

	/** The subgraph executor. */
	public static ICloudSubgraphExecutor subgraphExecutor;

	/**
	 * replace index scan operator with SubgraphContainer.
	 * 
	 * @param indexScan
	 *            the index scan operator
	 */
	private void replaceIndexScanOperatorWithSubGraphContainer(
			final BasicOperator indexScan) {

		try {

			// Neuen Container erzeugen + inneren neuen rootNode
			final Root rootNodeOfOuterGraph = (Root) indexScan
					.getPrecedingOperators().get(0);

			// leere Liste einfügen, weil sonst NullpointerException - bug?
			rootNodeOfOuterGraph.setUnionVariables(new ArrayList<Variable>());

			final Root rootNodeOfSubGraph = rootNodeOfOuterGraph
					.newInstance(rootNodeOfOuterGraph.dataset);
			final CloudSubgraphContainer container = new CloudSubgraphContainer(
					rootNodeOfSubGraph, subgraphExecutor);
			final HashSet<Variable> variables = new HashSet<Variable>(
					indexScan.getIntersectionVariables());

			container.setUnionVariables(variables);
			container.setIntersectionVariables(variables);

			// alte Verbindungen merken
			final Collection<BasicOperator> preds = indexScan
					.getPrecedingOperators();
			List<OperatorIDTuple> succs = indexScan.getSucceedingOperators();

			for (final BasicOperator pred : preds) {
				pred.getOperatorIDTuple(indexScan).setOperator(container);
			}

			// Füge IndexscannOperator zum Caintainerhinzu und lösche alte
			// Nachfolger vom Indexscan
			rootNodeOfSubGraph.setSucceedingOperator(new OperatorIDTuple(
					indexScan, 0));
			indexScan.setSucceedingOperators(null);

			// Füge Resultoperator hinzu
			OperatorGraphHelper
					.getLastOperator(rootNodeOfSubGraph)
					.setSucceedingOperator(new OperatorIDTuple(new Result(), 0));

			// Container mit den Nachfolgern verbinden
			container.setSucceedingOperators(succs);

			// iterate through the new predecessors of the successors of the
			// original index scan operators and set new SubgraphContainer
			for (final OperatorIDTuple succ : succs) {
				succ.getOperator().removePrecedingOperator(indexScan);
				succ.getOperator().addPrecedingOperator(container);
			}

		} catch (final TriplePatternNotSupportedError e1) {
			System.err.println(e1);
			e1.printStackTrace();
		}
	}

	/** The current operator. */
	private BasicOperator currentOperator = null;

	/**
	 * _check private0.
	 * 
	 * @param _op
	 *            the _op
	 * @return true, if successful
	 */
	private boolean _checkPrivate0(final BasicOperator _op) {
		if (!(_op instanceof IndexScanContainer || _op instanceof MultiIndexScanContainer)) {
			return false;
		}

		this.currentOperator = _op;

		return true;
	}

	/**
	 * Instantiates a new adds the sub graph container rule.
	 */
	public AddSubGraphContainerRule() {
		this.startOpClass = BasicOperator.class;
		this.ruleName = "AddSubGraphContainer";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lupos.optimizations.logical.rules.generated.runtime.Rule#check(lupos.
	 * engine.operators.BasicOperator)
	 */
	@Override
	protected boolean check(final BasicOperator _op) {
		return this._checkPrivate0(_op);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lupos.optimizations.logical.rules.generated.runtime.Rule#replace(java
	 * .util.HashMap)
	 */
	@Override
	protected void replace(
			final HashMap<Class<?>, HashSet<BasicOperator>> _startNodes) {
		this.replaceIndexScanOperatorWithSubGraphContainer(this.currentOperator);

	}
}
