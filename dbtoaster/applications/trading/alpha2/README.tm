<TeXmacs|1.0.7.3>

<style|generic>

<\body>
  <section|Running the project>

  The different components of the project and what they can do as of now have
  been explained in the Documentation.pdf. This document assumes that points
  mentioned in that document are clear before proceeding with this one. There
  are many scenarios one may want to test, and how to test may not have been
  clear from the documentation. This document is to clear that. The following
  are the scenarios that have been tested, and the method to do the same has
  been mentioned.\ 

  <\description>
    <item*|The Stock Market Server>Command:<htab|5mm><htab|5mm><htab|5mm><htab|5mm><htab|5mm><htab|5mm><htab|5mm><htab|5mm><htab|5mm><htab|5mm><htab|5mm><htab|5mm><htab|5mm><htab|5mm>
    \ \ <code*|java -cp StockExchangeSimOpt.jar
    stockexchangesim.StockMarketServer \<less\>port\<gtr\>>. Here port is the
    command line argument. If left empty, the market is hosted at a default
    port of 8080.

    <item*|The BasicSobiTrader>There is a class for implementing a
    BasicSobiTrader. It can be run with the command: <code*|java -cp
    StockExchangeSimOpt.jar algotraders.basicsobitrader.BasicSobiTrader
    \<less\>param\<gtr\> \<less\>volToTrade\<gtr\> \<less\>margin\<gtr\>>.
    The purpose of the parameters in clarified in the code. The default
    values are 10.0, 100, 1.0.

    <item*|ProfitTrader>This was an extension to BasicSobiTrader to give
    meaning to portfolio management. It however has no algorithm for
    generating trades, hence the command line arguments to it specified in
    the code are meaningless till the algorithm is implemented. For now it
    trades in the market by the user typing in commands from the command
    line. Command:<code*|<code*|java -cp StockExchangeSimOpt.jar
    algotraders.profitbasedtrader.ProfitTrader>>.

    <item*|HistoricTrader>This program is to simulate history. To sync
    historic timestamps with current timestamps, uncomment the while loop for
    syncing in the code and rebuild the project. Command: <code*|java -cp
    StockExchangeSimOpt.jar algotraders.HistoricTrader>

    <item*|MarketMaker>Used to create the market maker. Command: <code*|java
    -cp StockExchangeSimOpt.jar nasdaq.MarkerMaker \<less\>spread\<gtr\>
    \<less\>maxVolTradeable\<gtr\> \<less\>windowLength\<gtr\>
    \<less\>stock\<gtr\>>. Default values are 0.1, 1000, 1000, 10101.\ 

    <item*|Broker>This framework is still incomplete hence its main has not
    been defined.\ 
  </description>
</body>

<\references>
  <\collection>
    <associate|auto-1|<tuple|1|?>>
  </collection>
</references>

<\auxiliary>
  <\collection>
    <\associate|toc>
      <vspace*|1fn><with|font-series|<quote|bold>|math-font-series|<quote|bold>|Running
      the project> <datoms|<macro|x|<repeat|<arg|x>|<with|font-series|medium|<with|font-size|1|<space|0.2fn>.<space|0.2fn>>>>>|<htab|5mm>>
      <no-break><pageref|auto-1><vspace|0.5fn>
    </associate>
  </collection>
</auxiliary>