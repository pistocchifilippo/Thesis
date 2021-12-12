package implicitAggregation.scenarios

import implicitAggregation.dsl.Scenario
import implicitAggregation.model.{AggregatingFunction, Attribute, Concept, IdFeature, Level, Measure, Wrapper}

class OneDimensionThreeLevel extends Scenario {

  scenario{
    "OneDimensionThreeLevel"
  }

  val CITY = IdFeature("City")
  val REGION = IdFeature("Region")
  val COUNTRY = IdFeature("Country")
  val REVENUE = Measure("Revenue")

  targetGraph{
    Concept("Sales")
      .hasFeature {
        REVENUE
      }
      .->("location") {
        Level("City")
          .hasFeature{
            CITY
          }
          .partOf{
            Level("Region")
              .hasFeature {
                REGION
              }
              .partOf {
                Level("Country")
                  .hasFeature {
                    COUNTRY
                  }
              }
          }
      }
  }

  wrapper{
    Wrapper("W3")
      .hasAttribute {
        Attribute("city") sameAs CITY
      }
      .hasAttribute {
        Attribute("revenue3") sameAs REVENUE
      }
  }

  wrapper{
    Wrapper("W1")
      .hasAttribute {
        Attribute("country") sameAs COUNTRY
      }
      .hasAttribute {
        Attribute("revenue") sameAs REVENUE
      }
  }

  wrapper{
    Wrapper("W2")
      .hasAttribute {
        Attribute("region") sameAs REGION
      }
      .hasAttribute {
        Attribute("revenue2") sameAs REVENUE
      }
  }

  wrapper{
    Wrapper("LUT1")
      .hasAttribute {
        Attribute("city1") sameAs CITY
      }
      .hasAttribute {
        Attribute("region1") sameAs REGION
      }
  }

  wrapper{
    Wrapper("LUT2")
      .hasAttribute {
        Attribute("region2") sameAs REGION
      }
      .hasAttribute {
        Attribute("country1") sameAs COUNTRY
      }
  }

  aggregation{
    AggregatingFunction("avg") aggregates REVENUE
  }

}
object OneDimensionThreeLevel extends App{
  new OneDimensionThreeLevel().run(false)
}
