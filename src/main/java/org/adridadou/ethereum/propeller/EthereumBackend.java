package org.adridadou.ethereum.propeller;

import org.adridadou.ethereum.propeller.event.BlockInfo;
import org.adridadou.ethereum.propeller.event.EthereumEventHandler;
import org.adridadou.ethereum.propeller.solidity.SolidityEvent;
import org.adridadou.ethereum.propeller.values.*;
import org.web3j.protocol.core.DefaultBlockParameter;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Created by davidroon on 20.01.17.
 * This code is released under Apache 2 license
 */
public interface EthereumBackend {

    CompletableFuture<GasPrice> getGasPrice();

    CompletableFuture<EthValue> getBalance(EthAddress address);

    CompletableFuture<Boolean> addressExists(EthAddress address);

    CompletableFuture<EthHash> submit(TransactionRequest request, Nonce nonce);

    CompletableFuture<GasUsage> estimateGas(EthAccount account, EthAddress address, EthValue value, EthData data);

    CompletableFuture<Nonce> getNonce(EthAddress currentAddress);

    CompletableFuture<BigInteger> getCurrentBlockNumber();

    CompletableFuture<Optional<BlockInfo>> getBlock(long blockNumber);

    CompletableFuture<Optional<BlockInfo>> getBlock(EthHash blockNumber);

    CompletableFuture<SmartContractByteCode> getCode(EthAddress address);

    CompletableFuture<EthData> constantCall(EthAccount account, EthAddress address, EthValue value, EthData data);

    List<EventData> logCall(final DefaultBlockParameter fromBlock, final DefaultBlockParameter toBlock, final SolidityEvent eventDefinition, EthAddress address, final String... optionalTopics);

    void register(EthereumEventHandler eventHandler);

    CompletableFuture<Optional<TransactionInfo>> getTransactionInfo(EthHash hash);

    ChainId getChainId();
}
