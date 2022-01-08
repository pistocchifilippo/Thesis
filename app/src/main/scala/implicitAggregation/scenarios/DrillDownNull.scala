package implicitAggregation.scenarios

import implicitAggregation.dsl.Scenario
import implicitAggregation.model._

class DrillDownNull extends Scenario {
  scenario{
    "DrillDownNull"
  }

  val REGION = IdFeature("Region")
  val COUNTRY = IdFeature("Country")
  val REVENUE = Measure("Revenue")

  targetGraph{
    Concept("Sales")
      .hasFeature {
        REVENUE
      }
      .->("location") {
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

  wrapper{
    Wrapper("W1")
      .hasAttribute {
        Attribute("region2") sameAs REGION
      }
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
    Wrapper("LUT")
      .hasAttribute {
        Attribute("region1") sameAs REGION
      }
      .hasAttribute {
        Attribute("country1") sameAs COUNTRY
      }
  }

}

object DrillDownNull extends App {
  new DrillDownNull().run(false)
}

