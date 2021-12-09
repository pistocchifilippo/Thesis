package implicitAggregation.dsl

import implicitAggregation.model._

trait ScenarioBuilder {
  def scenario(scenario: String): Unit
  def targetGraph(concept: Concept): Unit
  def wrapper(wrapper:Wrapper): Unit
  def aggregation(agg: AggregatingFunction): Unit
  def query(concept: Concept): Unit
  def run(executeImplicitRollUp: Boolean)
}

class Scenario extends ScenarioBuilder {
  private var scenarioName: String = null
  private var MG: Concept = null
  private var W: Set[Wrapper] = Set.empty
  private var AF: Set[AggregatingFunction] = Set.empty
  private var Q: Concept = null

  override def scenario(scenario: String): Unit = scenarioName = scenario

  override def targetGraph(concept: Concept): Unit = MG = concept

  override def wrapper(wrapper: Wrapper): Unit = W = W + wrapper

  override def aggregation(agg: AggregatingFunction): Unit = AF = AF + agg

  override def query(concept: Concept): Unit = Q = concept

  override def run(executeImplicitRollUp: Boolean): Unit = {
    Utils.generateAllFiles(Set(MG),W,Q,MG)(scenarioName)
    QueryExecution.execute(scenarioName,Utils.SCENARIOS_PATH,executeImplicitRollUp)(Q)(AF)(W)
  }
}