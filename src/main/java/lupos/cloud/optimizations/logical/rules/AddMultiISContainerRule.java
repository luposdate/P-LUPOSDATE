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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import lupos.cloud.operator.IndexScanContainer;
import lupos.cloud.operator.MultiIndexScanContainer;
import lupos.distributed.query.operator.withouthistogramsubmission.QueryClientRoot;
import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.OperatorIDTuple;
import lupos.engine.operators.multiinput.MultiInputOperator;
import lupos.optimizations.logical.rules.generated.runtime.Rule;

/**
 * Regel 2: Erzeugt für jeden MultiInput-Operator einen eigenen Container.
 * 
 */
public class AddMultiISContainerRule extends Rule {

	/** Liste die aller IndexScanContainer im Operatorgraphen. */
	ArrayList<BasicOperator> containerList;

	/** The finish. */
	private static boolean finish = false;

	/**
	 * Replace index scan operator with sub graph container.
	 * 
	 * @param qcRoot
	 *            the qc root
	 */
	private void replaceIndexScanOperatorWithSubGraphContainer(
			QueryClientRoot qcRoot) {
		int finalContainerSize = 1;
		// Am Anfang werden alle IndexScansContainer und MultiIndexScanContainer
		// in die Liste gepackt
		containerList = new ArrayList<BasicOperator>();
		for (OperatorIDTuple op : qcRoot.getSucceedingOperators()) {
			if (op.getOperator() instanceof IndexScanContainer) {
				if (((IndexScanContainer) op.getOperator())
						.isOneOperatorWasNotSupported()) {
					finalContainerSize++;
				}
				containerList.add(op.getOperator());
			}
			if (op.getOperator() instanceof MultiIndexScanContainer) {
				if (((MultiIndexScanContainer) op.getOperator())
						.isOneOperatorWasNotSupported()) {
					finalContainerSize++;
				}
				containerList.add(op.getOperator());
			}
		}

		// Die IndexScans werden nun so lange gemerged bis nur noch ein
		// Container existiert

		if (containerList.size() <= finalContainerSize) {
			finish = true;
		} else {
			mergeContainer();
		}
	}

	/**
	 * Merge container.
	 */
	private void mergeContainer() {
		HashMap<BasicOperator, LinkedList<BasicOperator>> mergeMap = new HashMap<BasicOperator, LinkedList<BasicOperator>>();
		// Für jeden (Multi-)Index-Container wird die Nachfolge MultiInput
		// Operation gesucht und in die mergeMap gepackt. Dort befinet sich
		// danach die MultiInputOperation also z.B. Union und eine Lister der
		// beteiligten (Multi-)IndexScan Operationen
		for (BasicOperator op : containerList) {
			if (op instanceof IndexScanContainer) {
				if (((IndexScanContainer) op).isOneOperatorWasNotSupported()) {
					continue;
				}
			}
			if (op instanceof MultiIndexScanContainer) {
				if (((MultiIndexScanContainer) op)
						.isOneOperatorWasNotSupported()) {
					continue;
				}
			}
			for (OperatorIDTuple path : op.getSucceedingOperators()) {
				BasicOperator foundOp = OperatorGraphHelper
						.getNextMultiInputOperation(path.getOperator(),
								path.getId());
				if (foundOp != null) {
					LinkedList<BasicOperator> list = mergeMap.get(foundOp);
					if (list == null) {
						list = new LinkedList<BasicOperator>();
						list.add(op);
						mergeMap.put(foundOp, list);
					} else {
						Integer position = OperatorGraphHelper
								.getLastEdgeNumber();
						while (position > list.size() && position > 0) {
							position--;
						}
						list.add(position, op);
					}
				}
			}
		}

		// Für jeden MultiInputOperator wird nun ein eigner Container erstellt
		// mit allen dazugehörigen (Multi-)Index-Scan Containern
		for (BasicOperator multiInputOperator : mergeMap.keySet()) {
			LinkedList<BasicOperator> toMerge = new LinkedList<BasicOperator>(
					mergeMap.get(multiInputOperator));
			// Eine MultiInput Operator braucht immer mehr als eine Input
			// Operation
			if (toMerge.size() > 1) {
				MultiIndexScanContainer multiIndexContainer = new MultiIndexScanContainer();

				// Füge Union/Intersection-Variablen hinzu
				multiIndexContainer.setUnionVariables(OperatorGraphHelper
						.getUnionVariablesFromMultipleOperations(toMerge));
				multiIndexContainer
						.setIntersectionVariables(OperatorGraphHelper
								.getIntersectionVariablesFromMultipleOperations(toMerge));

				// Neuen Container erzeugen und den MultiInput Operator und
				// (Multi-)IndexScans übergeben
				multiIndexContainer.addSubContainer(
						(MultiInputOperator) multiInputOperator, toMerge);

				// Wenn der MultiInputOperator von Variablen abhängt
				// müssen die als Projektion in den Containern
				// hinzugefügt werden
				OperatorGraphHelper
						.addProjectionFromMultiInputOperatorInContainerIfNecessary(
								multiInputOperator, toMerge);

				// Füge Operatoren zum Container hinzu
				boolean oneOperationWasNotSupported = false;
				for (BasicOperator op : OperatorGraphHelper
						.getAndDeleteOperationUntilNextMultiInputOperator(multiInputOperator
								.getSucceedingOperators())) {
					if (OperatorGraphHelper.isOperationSupported(op)
							&& !oneOperationWasNotSupported) {
						// Wenn die Operation unterstützt wird füge zum
						// Container hinzu
						multiIndexContainer.addOperator(op);
						// Falls eine Operatione z.B. eine Projektion/Filter von
						// Variablen abhängig ist füge diese zur inneren
						// Container-
						// Projektion hinzu
						OperatorGraphHelper.addProjectionIfNecessary(op,
								containerList);
					} else {
						// Ansonsten hänge die Operation hinter den Container.
						// Alle Folgeoperationen werden dann, obwohl sie
						// vielleichtsogar unterstützt wreden, auch dahitner
						// gehängt, weil
						// sonst die Reihenfolge durcheinander gebracht werden
						// würde
						OperatorGraphHelper.insertNewOperator(
								OperatorGraphHelper
										.getLastOperator(multiIndexContainer),
								op);
						oneOperationWasNotSupported = true;
						multiIndexContainer.oneOperatorWasNotSupported(true);
					}
				}

				// Entferne den Container aus der Container Liste, wenn dieser
				// nicht noch für eine andere MultiInput-Operation gebraucht
				// wird.
				HashSet<BasicOperator> toRemove = new HashSet<BasicOperator>();
				for (BasicOperator container : toMerge) {
					OperatorGraphHelper.removeDuplicatedEdges(container);
					if (container.getSucceedingOperators().size() == 1) {
						containerList.remove(container);
						toRemove.add(container);
					} else {
						multiInputOperator.removePrecedingOperator(container);
						container.removeSucceedingOperator(multiInputOperator);
					}
				}

				OperatorGraphHelper.mergeContainerListIntoOneNewContainer(
						multiIndexContainer, toRemove);

				multiInputOperator.removeFromOperatorGraph();

				containerList.add(multiIndexContainer);

			}
		}

	}

	/** The index scan. */
	private QueryClientRoot indexScan = null;

	/**
	 * _check private0.
	 * 
	 * @param _op
	 *            the _op
	 * @return true, if successful
	 */
	private boolean _checkPrivate0(final BasicOperator _op) {

		// workaround, nicht schön - aber funktioniert :)
		if (!(_op instanceof QueryClientRoot)) {
			return false;
		} else {
			if (indexScan == null) {
				this.indexScan = (QueryClientRoot) _op;
				return true;
			} else {
				return !finish;
			}
		}
	}

	/**
	 * Reset.
	 */
	public static void reset() {
		finish = false;
	}

	/**
	 * Instantiates a new adds the multi is container rule.
	 */
	public AddMultiISContainerRule() {
		this.startOpClass = QueryClientRoot.class;
		this.ruleName = "AddMergeContainer";
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
		this.replaceIndexScanOperatorWithSubGraphContainer(this.indexScan);

	}
}
