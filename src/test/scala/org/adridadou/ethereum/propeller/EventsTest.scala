package org.adridadou.ethereum.propeller

import java.io.File
import java.util.Optional
import java.util.concurrent.CompletableFuture

import com.google.common.collect.Lists
import org.adridadou.ethereum.propeller.backend.{EthereumTest, TestConfig}
import org.adridadou.ethereum.propeller.exception.EthereumApiException
import org.adridadou.ethereum.propeller.keystore.AccountProvider
import org.adridadou.ethereum.propeller.solidity.SolidityContractDetails
import org.adridadou.ethereum.propeller.values.EthValue.ether
import org.adridadou.ethereum.propeller.values._
import org.scalatest.check.Checkers
import org.scalatest.{FlatSpec, Matchers}

import scala.compat.java8.OptionConverters._

/**
  * Created by davidroon on 11.04.17.
  * This code is released under Apache 2 license
  */
class EventsTest extends FlatSpec with Matchers with Checkers {

  private val mainAccount = AccountProvider.fromSeed("hello")
  private val ethereum = CoreEthereumFacadeProvider.create(new EthereumTest(TestConfig.builder.balance(mainAccount, ether(1000)).build), EthereumConfig.builder().build())
  private val contractSource = SoliditySource.from(new File("src/test/resources/contractEvents.sol"))
  private val address = publishAndMapContract(ethereum)

  "Events" should "be observable from the ethereum network" in {
    (for (compiledContract <- ethereum.compile(contractSource).findContract("contractEvents").asScala;
          solidityEvent <- ethereum.findEventDefinition(compiledContract, "MyEvent", classOf[MyEvent]).asScala) yield {
      val myContract = ethereum.createContractProxy(compiledContract, address, mainAccount, classOf[ContractEvents])
      val observeEventWithInfo = ethereum.observeEventsWithInfo(solidityEvent, address)

      myContract.createEvent("my event is here and it is much longer than anticipated")
      val result = observeEventWithInfo.first(new EventInfo(EthHash.empty(), new EmptyEvent())).toFuture.get()
      result.getTransactionHash shouldBe EthHash.of("16a483684071817f67b4375becca013d17b969afaa8c431c0426b561e83d7bf0")
      result.getResult.value shouldBe "my event is here and it is much longer than anticipated"

      ethereum.getEventsAtBlock(ethereum.getTransactionInfo(result.getTransactionHash).get().getBlockHash, solidityEvent, address)

    }).asJava.orElseThrow(() => new EthereumApiException("something went wrong!"))
  }

  it should "work with a generic list as well" in {
    (for (compiledContract:SolidityContractDetails <- ethereum.compile(contractSource).findContract("contractEvents").asScala;
          solidityEvent    <- ethereum.findEventDefinitionForParameters(compiledContract, "MyEvent", Lists.newArrayList(classOf[EthAddress], classOf[EthAddress], classOf[String], classOf[EthData], classOf[EthSignature])).asScala) yield {

      val myContract = ethereum.createContractProxy(compiledContract, address, mainAccount, classOf[ContractEvents])
      val observeEventWithInfo = ethereum.observeEventsWithInfo(solidityEvent, address)

      myContract.createEvent("my event is here and it is much longer than anticipated")
      val emptyList:java.util.List[Any] = Lists.newArrayList()
      val result = observeEventWithInfo.first(new EventInfo(EthHash.empty(), emptyList)).toFuture.get()
      result.getTransactionHash shouldBe EthHash.of("98e7a7e23bf648887b69f641756872c1a5a2f5d63caaf11d4ddd551e980e8776")
      result.getResult.get(2) shouldBe "my event is here and it is much longer than anticipated"

      val events = ethereum.getEventsAtBlock(ethereum.getTransactionInfo(result.getTransactionHash).get().getBlockHash, solidityEvent, address)
      println(events)

    }).asJava.orElseThrow(() => new EthereumApiException("something went wrong!"))
  }

  private def publishAndMapContract(ethereum: EthereumFacade) = {
    val compiledContract = ethereum.compile(contractSource).findContract("contractEvents").get
    val futureAddress = ethereum.publishContract(compiledContract, mainAccount)
    futureAddress.get
  }
}

trait ContractEvents {
  def createEvent(value: String): CompletableFuture[Void]
}

case class MyEvent(from: EthAddress, to: EthAddress, value: String, ethData: EthData, signature: EthSignature)

class EmptyEvent() extends MyEvent(EthAddress.empty(), EthAddress.empty(), "", EthData.empty(), EthSignature.of(EthData.empty()))