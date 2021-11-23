package implicitAggregation.scenarios.meeting_23_11_2021

import implicitAggregation.model.{AggregatingFunction, Attribute, Concept, IdFeature, Level, Measure, QueryExecution, Utils, Wrapper}

// SMALLER GRANULARITY LEVEL QUERY EXAMPLE
object ErrorSimpleQuery2 extends App {

  val scenario = "ErrorSimpleQuery2"

  // GRAPH DEFINITION

  // Features
  val REGION = IdFeature("Region")
  val COUNTRY = IdFeature("Country")
  val REVENUE = Measure("Revenue")

  // Graph hierarchy
  val Sales =
    Concept("Sales")
      .hasFeature{REVENUE}
      .->("location"){
        Level("Region")
          .hasFeature{REGION}
          .partOf {
            Level("Country")
              .hasFeature {
                COUNTRY
              }
          }
      }
  val q =
    Concept("Sales")
      .hasFeature{REVENUE}
      .->("location"){
        Level("Region")
          .hasFeature{REGION}
      }
  val avg = AggregatingFunction("avg") aggregates REVENUE

  // Wrappers
  val w1 =
    Wrapper("W1")
      .hasAttribute{Attribute("country") sameAs COUNTRY}
      .hasAttribute{Attribute("revenue") sameAs REVENUE}

  val w2 =
    Wrapper("W2")
      .hasAttribute{Attribute("region") sameAs REGION}
      .hasAttribute{Attribute("revenue2") sameAs REVENUE}

  val w4 =
    Wrapper("LUT")
      .hasAttribute{Attribute("region1") sameAs REGION}
      .hasAttribute{Attribute("country1") sameAs COUNTRY}

  // WRITERS
  Utils.generateAllFiles(Set(Sales),Set(w1,w2,w4),q,Sales)(scenario)
  QueryExecution.execute(scenario,Utils.SCENARIOS_PATH,makeImplicitAggregation = false)(q)(Set(avg))(Set(w1,w2,w4))


}
