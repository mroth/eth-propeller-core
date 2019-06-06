package org.adridadou.ethereum.propeller.rpc;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import io.reactivex.Observable;
import io.reactivex.Single;
import org.adridadou.ethereum.propeller.EthereumBackend;
import org.adridadou.ethereum.propeller.event.BlockInfo;
import org.adridadou.ethereum.propeller.event.EthereumEventHandler;
import org.adridadou.ethereum.propeller.solidity.SolidityEvent;
import org.adridadou.ethereum.propeller.values.*;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.crypto.SECP256K1;
import org.apache.tuweni.eth.Address;
import org.apache.tuweni.units.bigints.UInt256;
import org.apache.tuweni.units.ethereum.Gas;
import org.apache.tuweni.units.ethereum.Wei;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.Log;
import org.web3j.protocol.core.methods.response.Transaction;

/**
 * Created by davidroon on 20.01.17.
 * This code is released under Apache 2 license
 */
public class EthereumRpc implements EthereumBackend {
    private static final Logger logger = LoggerFactory.getLogger(EthereumRpc.class);

    private final Web3JFacade web3JFacade;
    private final EthereumRpcEventGenerator ethereumRpcEventGenerator;
    private final ChainId chainId;

    public EthereumRpc(Web3JFacade web3JFacade, ChainId chainId, EthereumRpcConfig config) {
        this.web3JFacade = web3JFacade;
        this.ethereumRpcEventGenerator = new EthereumRpcEventGenerator(web3JFacade, config, this);
        this.chainId = chainId;
    }

    @Override
    public CompletableFuture<GasPrice> getGasPrice() {
        return web3JFacade.getGasPrice();
    }

    @Override
    public CompletableFuture<EthValue> getBalance(EthAddress address) {
        return web3JFacade.getBalance(address);
    }

    @Override
    public CompletableFuture<Boolean> addressExists(EthAddress address) {
        return web3JFacade.getTransactionCount(address)
                .thenCompose(result -> {
                    if(result.compareTo(BigInteger.ZERO) > 0) {
                        return CompletableFuture.completedFuture(true);
                    } else {
                        return web3JFacade.getBalance(address).thenCompose(balance -> {
                            if(!balance.isZero()) {
                                return CompletableFuture.completedFuture(true);
                            } else {
                                return web3JFacade.getCode(address).thenApply(SmartContractByteCode::isEmpty);
                            }
                        });
                    }
        });
    }

    @Override
    public CompletableFuture<EthHash> submit(TransactionRequest request, Nonce nonce) {
        return getGasPrice().thenCompose(gasPrice -> {
            org.apache.tuweni.eth.Transaction transaction = createTransaction(nonce, gasPrice, request);
            return web3JFacade.sendTransaction(EthData.of(transaction.toBytes().toArray()))
                    .thenApply(r -> EthHash.of(transaction.hash().toBytes().toArray()));
        });
    }

    private org.apache.tuweni.eth.Transaction createTransaction(Nonce nonce, GasPrice gasPrice, TransactionRequest request) {
        UInt256 nonceInt = UInt256.valueOf(nonce.getValue());
        Wei gasPriceWei = Wei.valueOf(gasPrice.getPrice().inWei());
        Gas gasLimitWei = Gas.valueOf(request.getGasLimit().getUsage());
        Wei value = Wei.valueOf(request.getValue().inWei());
        Bytes payload = Bytes.of(request.getData().data);
        SECP256K1.KeyPair keyPair = SECP256K1.KeyPair.fromSecretKey(SECP256K1.SecretKey.fromInteger(request.getAccount().getBigIntPrivateKey()));
        if (request.getAddress().isEmpty()) {
            Address address = null;
            //the signature gets generated when the Transaction is created
            return new org.apache.tuweni.eth.Transaction(nonceInt, gasPriceWei, gasLimitWei,
                    address, value, payload, keyPair, chainId.id);
        }
        else {
            Address address = Address.fromBytes(Bytes.of(request.getAddress().toData().data));
            return new org.apache.tuweni.eth.Transaction(nonceInt, gasPriceWei, gasLimitWei,
                    address, value, payload, keyPair, chainId.id);
        }
    }

    @Override
    public CompletableFuture<GasUsage> estimateGas(EthAccount account, EthAddress address, EthValue value, EthData data) {
        return web3JFacade.estimateGas(account, address, value, data).thenApply(GasUsage::new);
    }

    @Override
    public CompletableFuture<Nonce> getNonce(EthAddress currentAddress) {
        return web3JFacade.getTransactionCount(currentAddress).thenApply(Nonce::new);
    }

    @Override
    public CompletableFuture<BigInteger> getCurrentBlockNumber() {
        return web3JFacade.getCurrentBlockNumber();
    }

    @Override
    public CompletableFuture<Optional<BlockInfo>> getBlock(long number) {
        return web3JFacade.getBlock(number).thenCompose(this::toBlockInfo);
    }

    @Override
    public CompletableFuture<Optional<BlockInfo>> getBlock(EthHash ethHash) {
        return web3JFacade.getBlock(ethHash).thenCompose(this::toBlockInfo);
    }

    @Override
    public CompletableFuture<SmartContractByteCode> getCode(EthAddress address) {
        return web3JFacade.getCode(address);
    }

    @Override
    public CompletableFuture<EthData> constantCall(EthAccount account, EthAddress address, EthValue value, EthData data) {
        return web3JFacade.constantCall(account, address, data);
    }

    @Override
    public List<EventData> logCall(DefaultBlockParameter fromBlock, DefaultBlockParameter toBlock, SolidityEvent eventDefinition, EthAddress address, String... optionalTopics) {
        return web3JFacade.loggingCall(fromBlock, toBlock, eventDefinition, address, optionalTopics).stream().map(log -> toEventInfo(EthHash.of(log.getTransactionHash()), log)).collect(Collectors.toList());
    }

    @Override
    public void register(EthereumEventHandler eventHandler) {
        ethereumRpcEventGenerator.addListener(eventHandler);
    }

    private <T> Observable<T> toObservable(Optional<T> opt) {
        return Observable.fromIterable(opt.map(Collections::singleton)
                .orElseGet(Collections::emptySet));
    }

    @Override
    public CompletableFuture<Optional<TransactionInfo>> getTransactionInfo(EthHash hash) {
        return fromObservable(Observable.fromFuture(web3JFacade.getReceipt(hash))
            .flatMap(this::toObservable)
            .filter(web3jReceipt -> web3jReceipt.getBlockHash() != null) //Parity gives receipt even if not included yet
            .flatMap(web3jReceipt -> Observable.fromFuture(web3JFacade.getTransaction(hash))
                    .flatMap(this::toObservable)
                    .map(transaction -> {
                        TransactionReceipt receipt = toReceipt(transaction, web3jReceipt);
                        TransactionStatus status = transaction.getBlockHash().isEmpty() ? TransactionStatus.Unknown : TransactionStatus.Executed;
                        return Optional.of(new TransactionInfo(hash, receipt, status, EthHash.of(transaction.getBlockHash())));
                    })).single(Optional.empty()));
    }

    @Override
    public ChainId getChainId() {
        return chainId;
    }

    private  <T> CompletableFuture<T> fromObservable(Single<T> single) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        single
                .doOnError(future::completeExceptionally)
                .doOnSuccess(future::complete);
        return future;
    }

    CompletableFuture<Optional<BlockInfo>> toBlockInfo(Optional<EthBlock.Block> optionalBlock) {
        return optionalBlock.map(block -> {
            Map<String, EthBlock.TransactionObject> txObjects = block.getTransactions().stream()
                    .map(tx -> (EthBlock.TransactionObject) tx.get()).collect(Collectors.toMap(EthBlock.TransactionObject::getHash, e -> e));

            Map<String, org.web3j.protocol.core.methods.response.TransactionReceipt> initValue = new HashMap<>();

            Single<Map<String, org.web3j.protocol.core.methods.response.TransactionReceipt>> r = Observable.fromIterable(txObjects.values())
                    .flatMap(transactionObject -> Observable.fromFuture(web3JFacade.getReceipt(EthHash.of(transactionObject.getHash()))))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .filter(web3jReceipt -> web3jReceipt.getBlockHash() != null) //Parity gives receipt even if not included yet;
                    .collectInto(initValue, (receiptMap, value) -> receiptMap.put(value.getTransactionHash(), value));

            return fromObservable(r).thenApply(receiptMap -> {
                List<TransactionReceipt> receiptList = initValue.entrySet().stream()
                        .map(entry -> toReceipt(txObjects.get(entry.getKey()), entry.getValue())).collect(Collectors.toList());

                return Optional.of(new BlockInfo(block.getNumber().longValue(), receiptList));
            });
        }).orElse(CompletableFuture.completedFuture(Optional.empty()));
    }

    private TransactionReceipt toReceipt(Transaction tx, org.web3j.protocol.core.methods.response.TransactionReceipt receipt) {
        boolean successful = !receipt.getGasUsed().equals(tx.getGas());
        String error = "";
        if (!successful) {
            error = "All the gas was used! an error occurred";
        }

        return new TransactionReceipt(EthHash.of(receipt.getTransactionHash()), EthHash.of(receipt.getBlockHash()), EthAddress.of(receipt.getFrom()), EthAddress.of(receipt.getTo()), EthAddress.of(receipt.getContractAddress()), EthData.of(tx.getInput()), error, EthData.empty(), successful, toEventInfos(EthHash.of(receipt.getTransactionHash()), receipt.getLogs()), EthValue.wei(tx.getValue()));
    }

    private List<EventData> toEventInfos(EthHash transactionHash, List<Log> logs) {
        return logs.stream().map(log -> this.toEventInfo(transactionHash, log)).collect(Collectors.toList());
    }

    private EventData toEventInfo(EthHash transactionHash, Log log) {
        List<EthData> topics = log.getTopics().stream().map(EthData::of).collect(Collectors.toList());
        if(topics.size() > 0) {
            EthData eventSignature = topics.get(0);
            EthData eventArguments = EthData.of(log.getData());
            return new EventData(transactionHash, eventSignature, eventArguments, topics.subList(1, topics.size()));
        } else {
            return new EventData(transactionHash, EthData.empty(), EthData.empty(), new ArrayList<>());
        }
    }
}
