package org.adridadou.ethereum.propeller.rpc;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import io.reactivex.Flowable;
import org.adridadou.ethereum.propeller.exception.EthereumApiException;
import org.adridadou.ethereum.propeller.solidity.SolidityEvent;
import org.adridadou.ethereum.propeller.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.*;
import org.web3j.protocol.core.methods.request.EthFilter;
import org.web3j.protocol.core.methods.request.Transaction;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.Log;
import org.web3j.protocol.core.methods.response.TransactionReceipt;
import org.web3j.utils.Numeric;
import io.reactivex.Observable;

/**
 * Created by davidroon on 19.11.16.
 * This code is released under Apache 2 license
 */
public class Web3JFacade {
    private static final Logger logger = LoggerFactory.getLogger(Web3JFacade.class);
    private final Web3j web3j;
    private final Web3jBlockHandler blockEventHandler = new Web3jBlockHandler();
    private BigInteger lastBlockNumber = BigInteger.ZERO;

    public Web3JFacade(final Web3j web3j) {
        this.web3j = web3j;
    }

    CompletableFuture<EthData> constantCall(final EthAccount account, final EthAddress address, final EthData data) {
        return handleErrorFutureRequest(web3j.ethCall(new Transaction(
                account.getAddress().normalizedWithLeading0x(),
                null,
                null,
                null,
                address.normalizedWithLeading0x(),
                BigInteger.ZERO,
                data.toString()
        ), DefaultBlockParameterName.LATEST)).thenApply(EthData::of);
    }

    List<Log> loggingCall(SolidityEvent eventDefiniton, final EthAddress address, final String... optionalTopics) {
        EthFilter ethFilter = new EthFilter(DefaultBlockParameterName.EARLIEST, DefaultBlockParameterName.LATEST, address.withLeading0x());

        ethFilter.addSingleTopic(eventDefiniton.getDescription().signatureLong().withLeading0x());
        ethFilter.addOptionalTopics(optionalTopics);

        List<Log> list = new ArrayList<>();
        this.web3j.ethLogFlowable(ethFilter).subscribe(list::add).dispose();
        return list;
    }

    CompletableFuture<BigInteger> getTransactionCount(EthAddress address) {
        return handleErrorFutureRequest(web3j.ethGetTransactionCount(address.normalizedWithLeading0x(), DefaultBlockParameterName.LATEST))
            .thenApply(Numeric::decodeQuantity);
    }

    Flowable<EthBlock> observeBlocks() {
        return web3j.blockFlowable(true);
    }

    Observable<EthBlock> observeBlocksPolling(long pollingFrequence) {
        Executors.newCachedThreadPool().submit(() -> {
            while (true) {
                try {
                    EthBlock currentBlock = web3j
                            .ethGetBlockByNumber(DefaultBlockParameter.valueOf(DefaultBlockParameterName.LATEST.name()), true).send();
                    BigInteger currentBlockNumber = currentBlock.getBlock().getNumber();

                    if(currentBlockNumber.compareTo(this.lastBlockNumber) > 0) {

                        //Set last block to current block -1 in case last block is zero to prevent all blocks from being retrieved
                        if (this.lastBlockNumber.equals(BigInteger.ZERO)) {
                            this.lastBlockNumber = currentBlockNumber.subtract(BigInteger.ONE);
                        }

                        //In case the block number of the current block is more than 1 higher than the last block, retrieve intermediate blocks
                        for (BigInteger i = this.lastBlockNumber.add(BigInteger.ONE); i.compareTo(currentBlockNumber) < 0; i = i.add(BigInteger.ONE)) {
                            EthBlock missedBlock = web3j.ethGetBlockByNumber(DefaultBlockParameter.valueOf(i), true).send();
                            this.lastBlockNumber = i;
                            blockEventHandler.newElement(missedBlock);
                        }

                        this.lastBlockNumber = currentBlockNumber;
                        blockEventHandler.newElement(currentBlock);
                    }
                } catch (Throwable e) {
                    logger.warn("error while polling blocks", e);
                }
                Thread.sleep(pollingFrequence);
            }
        });
        return blockEventHandler.observable;
    }

    CompletableFuture<BigInteger> estimateGas(EthAccount account, EthAddress address, EthValue value, EthData data) {
            return handleErrorFutureRequest(web3j.ethEstimateGas(new Transaction(account.getAddress().normalizedWithLeading0x(), null, null, null,
                    address.isEmpty() ? null : address.normalizedWithLeading0x(), value.inWei(), data.toString())))
                    .thenApply(Numeric::decodeQuantity);
    }

    CompletableFuture<GasPrice> getGasPrice() {
        return handleErrorFutureRequest(web3j.ethGasPrice())
            .thenApply(Numeric::decodeQuantity)
            .thenApply(EthValue::wei)
            .thenApply(GasPrice::new);
    }

    CompletableFuture<EthHash> sendTransaction(final EthData rawTransaction) {
        return handleErrorFutureRequest(web3j.ethSendRawTransaction(rawTransaction.withLeading0x()))
                .thenApply(EthHash::of);
    }

    CompletableFuture<EthValue> getBalance(EthAddress address) {
        return handleErrorFutureRequest(web3j.ethGetBalance(address.normalizedWithLeading0x(), DefaultBlockParameterName.LATEST))
                .thenApply(result -> EthValue.wei(Numeric.decodeQuantity(result)));
    }

    CompletableFuture<SmartContractByteCode> getCode(EthAddress address) {
        return handleErrorFutureRequest(web3j.ethGetCode(address.normalizedWithLeading0x(), DefaultBlockParameterName.LATEST))
            .thenApply(SmartContractByteCode::of);
    }

    CompletableFuture<BigInteger> getCurrentBlockNumber() {
        return handleErrorFutureRequest(web3j.ethBlockNumber())
                .thenApply(Numeric::decodeQuantity);
    }

    CompletableFuture<Optional<TransactionReceipt>> getReceipt(EthHash hash) {
        return handleErrorFutureRequest(web3j.ethGetTransactionReceipt(hash.withLeading0x()))
                .thenApply(Optional::ofNullable);
    }

    CompletableFuture<Optional<org.web3j.protocol.core.methods.response.Transaction>> getTransaction(EthHash hash) {
        return handleErrorFutureRequest(web3j.ethGetTransactionByHash(hash.withLeading0x()))
                .thenApply(Optional::ofNullable);
    }

    CompletableFuture<Optional<EthBlock.Block>> getBlock(long blockNumber) {
        return handleErrorFutureRequest(web3j.ethGetBlockByNumber(new DefaultBlockParameterNumber(BigInteger.valueOf(blockNumber)), true))
                .thenApply(Optional::ofNullable);
    }

    CompletableFuture<Optional<EthBlock.Block>> getBlock(EthHash blockHash) {
        return handleErrorFutureRequest(web3j.ethGetBlockByHash(blockHash.withLeading0x(), true))
                .thenApply(Optional::ofNullable);
    }

    private <S, T extends Response<S>> CompletableFuture<S> handleErrorFutureRequest(final Request<?, T> request) {
        return request.sendAsync().thenCompose(response -> {
            if (response.hasError()) {
                return CompletableFuture.failedFuture(new EthereumApiException(response.getError().getMessage()));
            }
            return CompletableFuture.completedFuture(response.getResult());
        });
    }
}
