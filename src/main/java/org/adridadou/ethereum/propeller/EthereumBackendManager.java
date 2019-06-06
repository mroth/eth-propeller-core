package org.adridadou.ethereum.propeller;

import com.google.common.collect.Lists;
import io.reactivex.schedulers.Schedulers;
import org.adridadou.ethereum.propeller.event.BlockInfo;
import org.adridadou.ethereum.propeller.event.EthereumEventHandler;
import org.adridadou.ethereum.propeller.solidity.SolidityEvent;
import org.adridadou.ethereum.propeller.values.*;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class EthereumBackendManager {

	private EthereumBackend currentBackend;

	private final List<EthereumBackend> backends;

	public EthereumBackendManager(EthereumBackend ... backends) {
		this(Lists.newArrayList(backends));
	}

	public EthereumBackendManager(List<EthereumBackend> backends) {
		this.backends = backends;
		this.currentBackend = backends.get(0);

		startBackendMonitor();
	}

	private void startBackendMonitor() {
		Schedulers.newThread().schedulePeriodicallyDirect(this::checkBackend, 0, 5, TimeUnit.MINUTES);
	}

	/**
	 * we check if there is a backend that has 10 blocks or more than the current backend. If yes, we switch to it.
	 * The order in the list defines the priority.
	 * We should always try to keep the first one alive and use the other ones only has a fallback.
	 *
	 * The rule is then.
	 * get the block number of the first backend.
	 * If any other backend has a latest block number greater than the main number + 10, then switch to it (it means the main backend has an issue)
	 * otherwise, switch back to the main (or stay on it if we were already there)
	 */
	private void checkBackend() {
		/*
		 *TODO: re-implement that with non blocking
		 *
		EthereumBackend mainBackend = backends.get(0);
		long mainBlockNumber = mainBackend.getCurrentBlockNumber();

		currentBackend = backends.stream()
			.filter(backend -> backend.getCurrentBlockNumber() - 10 > mainBlockNumber)
			.findFirst().orElse(currentBackend);
		 */
	}

	private EthereumBackend getCurrentBackend() {
		return currentBackend;
	}

	CompletableFuture<GasPrice> getGasPrice() {
		return getCurrentBackend().getGasPrice();
	}

	CompletableFuture<EthValue> getBalance(EthAddress address) {
		return getCurrentBackend().getBalance(address);
	}

	CompletableFuture<Boolean> addressExists(EthAddress address) {
		return getCurrentBackend().addressExists(address);
	}

	CompletableFuture<EthHash> submit(TransactionRequest request, Nonce nonce) {
		return getCurrentBackend().submit(request, nonce);
	}

	CompletableFuture<GasUsage> estimateGas(EthAccount account, EthAddress address, EthValue value, EthData data) {
		return getCurrentBackend().estimateGas(account, address, value, data);
	}

	CompletableFuture<Nonce> getNonce(EthAddress currentAddress) {
		return getCurrentBackend().getNonce(currentAddress);
	}

	CompletableFuture<BigInteger> getCurrentBlockNumber() {
		return getCurrentBackend().getCurrentBlockNumber();
	}

	CompletableFuture<Optional<BlockInfo>> getBlock(long blockNumber) {
		return getCurrentBackend().getBlock(blockNumber);
	}

	CompletableFuture<Optional<BlockInfo>> getBlock(EthHash blockHash) {
		return getCurrentBackend().getBlock(blockHash);
	}

	CompletableFuture<SmartContractByteCode> getCode(EthAddress address) {
		return getCurrentBackend().getCode(address);
	}

	CompletableFuture<EthData> constantCall(EthAccount account, EthAddress address, EthValue value, EthData data) {
		return getCurrentBackend().constantCall(account, address, value, data);
	}

	List<EventData> logCall(final SolidityEvent eventDefinition, EthAddress address, final String... optionalTopics) {
		return getCurrentBackend().logCall(eventDefinition, address, optionalTopics);
	}

	void register(EthereumEventHandler eventHandler) {
		getCurrentBackend().register(eventHandler);
	}

	CompletableFuture<Optional<TransactionInfo>> getTransactionInfo(EthHash hash) {
		return getCurrentBackend().getTransactionInfo(hash);
	}

	ChainId getChainId() {
		return getCurrentBackend().getChainId();
	}
}
