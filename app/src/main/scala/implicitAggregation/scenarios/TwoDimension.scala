package implicitAggregation.scenarios
import implicitAggregation.dsl.Scenario
import implicitAggregation.model._

class TwoDimension extends Scenario {

  scenario {
    "TwoDimension"
  }

  val REGION = IdFeature("Region")
  val COUNTRY = IdFeature("Country")

  val NAME = IdFeature("Name")
  val CATEGORY = IdFeature("Category")

  val REVENUE = Measure("Revenue")

  targetGraph {
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
      .->("product") {
        Level("Name")
          .hasFeature {
            NAME
          }
          .partOf {
            Level("Category")
              .hasFeature {
                CATEGORY
              }
          }
      }

  }

  wrapper {
    Wrapper("W1")
      .hasAttribute {
        Attribute("country") sameAs COUNTRY
      }
      .hasAttribute {
        Attribute("revenue") sameAs REVENUE
      }
  }

  wrapper {
    Wrapper("W2")
      .hasAttribute {
        Attribute("region") sameAs REGION
      }
      .hasAttribute {
        Attribute("revenue2") sameAs REVENUE
      }
  }

  wrapper {
    Wrapper("LUT1")
      .hasAttribute {
        Attribute("region1") sameAs REGION
      }
      .hasAttribute {
        Attribute("country1") sameAs COUNTRY
      }
  }

  wrapper {
    Wrapper("W3")
      .hasAttribute {
        Attribute("name") sameAs NAME
      }
      .hasAttribute {
        Attribute("revenue3") sameAs REVENUE
      }
  }

  wrapper {
    Wrapper("W4")
      .hasAttribute {
        Attribute("category") sameAs CATEGORY
      }
      .hasAttribute {
        Attribute("revenue4") sameAs REVENUE
      }
  }

  wrapper {
    Wrapper("LUT2")
      .hasAttribute {
        Attribute("name1") sameAs NAME
      }
      .hasAttribute {
        Attribute("category1") sameAs CATEGORY
      }
  }

  aggregation {
    AggregatingFunction("avg") aggregates REVENUE
  }

}

object TwoDimension extends App {
  new TwoDimension().run(false)
}
