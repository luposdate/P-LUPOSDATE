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
package lupos.cloud.query;

import java.util.Date;
import java.util.List;

import lupos.cloud.operator.ICloudSubgraphExecutor;
import lupos.cloud.optimizations.logical.rules.AddMultiISContainerRule;
import lupos.cloud.optimizations.logical.rules.AddSubGraphContainerRule;
import lupos.cloud.optimizations.logical.rules.CloudRulePackage;
import lupos.cloud.storage.Storage_Cloud;
import lupos.cloud.storage.util.CloudManagement;
import lupos.datastructures.bindings.Bindings;
import lupos.datastructures.bindings.BindingsMap;
import lupos.datastructures.items.literal.LiteralFactory;
import lupos.distributed.query.QueryClient;
import lupos.distributed.query.operator.histogramsubmission.IHistogramExecutor;
import lupos.distributed.storage.IStorage;
import lupos.engine.operators.index.BasicIndexScan;
import lupos.misc.debug.BasicOperatorByteArray;
import lupos.optimizations.logical.rules.DebugContainer;
import lupos.rdf.Prefix;

/**
 * Luposdate Cloud Evaluator.
 */
public class CloudEvaluator extends QueryClient {

	/** The subgraph executor. */
	public final ICloudSubgraphExecutor subgraphExecutor;

	/**
	 * Instantiates a new cloud evaluator.
	 *
	 * @throws Exception
	 *             the exception
	 */
	public CloudEvaluator() throws Exception {
		// BindingsFactory in Storage_Cloud will be set in the constructor of QueryClient
		this(Storage_Cloud.getInstance(null), Storage_Cloud.getInstance(null)
				.getCloudManagement(), new Cloud_SubgraphExecutor(Storage_Cloud
				.getInstance(null).getCloudManagement()));
	}

	/**
	 * Instantiates a new cloud evaluator.
	 *
	 * @param args
	 *            the args
	 * @throws Exception
	 *             the exception
	 */
	public CloudEvaluator(final String[] args) throws Exception {
		// BindingsFactory in Storage_Cloud will be set in the constructor of QueryClient
		this(Storage_Cloud.getInstance(null), Storage_Cloud.getInstance(null)
				.getCloudManagement(), new Cloud_SubgraphExecutor(Storage_Cloud
				.getInstance(null).getCloudManagement()), args);
	}

	/**
	 * Instantiates a new cloud evaluator.
	 *
	 * @param storage
	 *            the storage
	 * @param cloudManagement
	 *            the cloud management
	 * @param subgraphExecutor
	 *            the subgraph executor
	 * @throws Exception
	 *             the exception
	 */
	public CloudEvaluator(final IStorage storage,
			final CloudManagement cloudManagement,
			final ICloudSubgraphExecutor subgraphExecutor) throws Exception {
		this(storage, null, cloudManagement, subgraphExecutor);
	}

	/**
	 * Instantiates a new cloud evaluator.
	 *
	 * @param storage
	 *            the storage
	 * @param histogramExecutor
	 *            the histogram executor
	 * @param cloudManagement
	 *            the cloud management
	 * @param subgraphExecutor
	 *            the subgraph executor
	 * @throws Exception
	 *             the exception
	 */
	public CloudEvaluator(final IStorage storage,
			final IHistogramExecutor histogramExecutor,
			final CloudManagement cloudManagement,
			final ICloudSubgraphExecutor subgraphExecutor) throws Exception {
		super(storage, histogramExecutor);
		this.subgraphExecutor = subgraphExecutor;
	}

	/**
	 * Instantiates a new cloud evaluator.
	 *
	 * @param storage
	 *            the storage
	 * @param cloudManagement
	 *            the cloud management
	 * @param subgraphExecutor
	 *            the subgraph executor
	 * @param args
	 *            the args
	 * @throws Exception
	 *             the exception
	 */
	public CloudEvaluator(final IStorage storage,
			final CloudManagement cloudManagement,
			final ICloudSubgraphExecutor subgraphExecutor, final String[] args)
			throws Exception {
		this(storage, null, cloudManagement, subgraphExecutor, args);
	}

	/**
	 * Instantiates a new cloud evaluator.
	 *
	 * @param storage
	 *            the storage
	 * @param histogramExecutor
	 *            the histogram executor
	 * @param cloudManagement
	 *            the cloud management
	 * @param subgraphExecutor
	 *            the subgraph executor
	 * @param args
	 *            the args
	 * @throws Exception
	 *             the exception
	 */
	public CloudEvaluator(final IStorage storage,
			final IHistogramExecutor histogramExecutor,
			final CloudManagement cloudManagement,
			final ICloudSubgraphExecutor subgraphExecutor, final String[] args)
			throws Exception {
		super(storage, histogramExecutor, args);
		this.subgraphExecutor = subgraphExecutor;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see lupos.distributed.query.QueryClient#initOptimization()
	 */
	@Override
	protected void initOptimization() {

		/**
		 * ANMERKUNG: Auskommentiert, denn es ist nicht notwendig den
		 * IndexScanOperator in verschiedene Operatoren aufzuteilen beim Cloud
		 * Ansatz!
		 */

		// if (this.histogramExecutor == null) {
		// // make binary joins such that subgraphs can be identified...
		this.opt = BasicIndexScan.NONE;
		// } else {
		// // use histograms to find best join order
		// this.opt = BasicIndexScan.BINARY;
		// }
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * lupos.engine.evaluators.BasicIndexQueryEvaluator#logicalOptimization()
	 */
	@Override
	public long logicalOptimization() {
		final long start = new Date().getTime();
		super.logicalOptimization();

		// evaluate rule to identify subgraphs and put them into a
		// SubgraphContainer operator
		AddSubGraphContainerRule.subgraphExecutor = this.subgraphExecutor;
		AddMultiISContainerRule.reset(); // unschön, bessere lösung finden
		final CloudRulePackage rules = new CloudRulePackage();
		rules.applyRules(this.rootNode);

		return new Date().getTime() - start;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see lupos.engine.evaluators.BasicIndexQueryEvaluator#
	 * logicalOptimizationDebugByteArray(lupos.rdf.Prefix)
	 */
	@Override
	public List<DebugContainer<BasicOperatorByteArray>> logicalOptimizationDebugByteArray(
			final Prefix prefixInstance) {
		final List<DebugContainer<BasicOperatorByteArray>> result = super
				.logicalOptimizationDebugByteArray(prefixInstance);

		// evaluate rule to identify subgraphs and put them into a
		// SubgraphContainer operator
		AddSubGraphContainerRule.subgraphExecutor = this.subgraphExecutor;
		AddMultiISContainerRule.reset(); // unschön, bessere lösung finden
		final CloudRulePackage rules = new CloudRulePackage();
		final List<DebugContainer<BasicOperatorByteArray>> result2 = rules
				.applyRulesDebugByteArray(this.rootNode, prefixInstance);
		if (result2 != null) {
			result.addAll(result2);
		}

		return result;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see lupos.engine.evaluators.BasicIndexQueryEvaluator#init()
	 */
	@Override
	public void init() throws Exception {
		// just for avoiding problems in distributed scenarios
		Bindings.instanceClass = BindingsMap.class;
		LiteralFactory.setType(LiteralFactory.MapType.NOCODEMAP);
		super.init();
	}

	public void shutdown() {
		Storage_Cloud.getInstance(this.bindingsFactory).getCloudManagement().shutdown();
	}

	public CloudManagement getCloudManagement() {
		return Storage_Cloud.getInstance(this.bindingsFactory).getCloudManagement();
	}
}
