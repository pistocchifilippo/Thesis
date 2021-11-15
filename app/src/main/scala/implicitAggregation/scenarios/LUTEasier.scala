package implicitAggregation.scenarios

import implicitAggregation.model._

object LUTEasier extends App {

  val basePath = "/Users/filippo/ImplicitAggregation/app/src/main/resources/scenarios/"
  val scenario = "LUTEasier"
  val configurationFilePath = basePath + scenario + "/"
  //  val configurationFilePath = "/Users/filippo/GraphBuilder/scenarios/LUT/"
  Utils.buildPath(configurationFilePath)

  // GRAPH DEFINITION

  // Features
  val CITY = IdFeature("City")
  val REGION = IdFeature("Region")
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
              .hasFeature{REGION}
          }
      }
  val avg = AggregatingFunction("avg") aggregates REVENUE

  // Wrappers
  val w2 =
    Wrapper("W2")
      .hasAttribute{Attribute("region") sameAs REGION}
      .hasAttribute{Attribute("revenue2") sameAs REVENUE}

  val w3 =
    Wrapper("W3")
      .hasAttribute{Attribute("city") sameAs CITY}
      .hasAttribute{Attribute("revenue3") sameAs REVENUE}

  val w5 =
    Wrapper("LUT2")
      .hasAttribute{Attribute("city1") sameAs CITY}
      .hasAttribute{Attribute("region2") sameAs REGION}


  // WRITERS
  Utils.generateAllFiles(Set(Sales),Set(w2,w3,w5),q)(configurationFilePath)
  QueryExecution.execute(scenario,basePath,makeImplicitAggregation = false)(q)(Set(avg))(Set(w2,w3,w5))

}
