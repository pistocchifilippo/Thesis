package implicitAggregation.scenarios.meeting_23_11_2021

import implicitAggregation.model.{AggregatingFunction, Attribute, Concept, IdFeature, Level, Measure, QueryExecution, Utils, Wrapper}

object ThreeLevelExample extends App {

  val scenario = "ThreeLevelExample"

  // GRAPH DEFINITION

  // Features
  val CITY = IdFeature("City")
  val REGION = IdFeature("Region")
  val COUNTRY = IdFeature("Country")
  val REVENUE = Measure("Revenue")

  // Graph hierarchy
  val Sales =
    Concept("Sales")
      .hasFeature{REVENUE}
      .->("location"){
        Level("City")
          .hasFeature{CITY}
          .partOf{
            Level("Region")
              .hasFeature{REGION}
              .partOf{
                Level("Country")
                  .hasFeature{COUNTRY}
              }
          }
      }
  val q =
    Concept("Sales")
      .hasFeature{REVENUE}
      .->("location"){
        Level("City")
          .hasFeature{CITY}
          .partOf{
            Level("Region")
              .partOf{
                Level("Country")
                  .hasFeature{COUNTRY}
              }
          }
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

  val w3 =
    Wrapper("W3")
      .hasAttribute{Attribute("city") sameAs CITY}
      .hasAttribute{Attribute("revenue3") sameAs REVENUE}

  val w4 =
    Wrapper("LUT")
      .hasAttribute{Attribute("region1") sameAs REGION}
      .hasAttribute{Attribute("country1") sameAs COUNTRY}

  val w5 =
    Wrapper("LUT2")
      .hasAttribute{Attribute("city1") sameAs CITY}
      .hasAttribute{Attribute("region2") sameAs REGION}


  // WRITERS
  Utils.generateAllFiles(Set(Sales),Set(w1,w2,w3,w4,w5),q)(scenario)
  QueryExecution.execute(scenario,Utils.SCENARIOS_PATH,makeImplicitAggregation = false)(q)(Set(avg))(Set(w1,w2,w3,w4,w5))


}