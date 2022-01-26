package implicitAggregation.scenarios.sales

import implicitAggregation.dsl.Scenario
import implicitAggregation.model.{Attribute, Concept, IdFeature, Level, Measure, Wrapper}

class GeographicCiRe extends Scenario {

  scenario{
    "GeographicCiRe"
  }

  val CITY = IdFeature("City_f")
  val REGION = IdFeature("Region_f")
  val REVENUE = Measure("Revenue_f")

  targetGraph(
    Concept("Sale")
      .hasFeature(REVENUE)
      .->("geog")(
        Level("City")
          .hasFeature(CITY)
          .partOf(
            Level("Region")
              .hasFeature(REGION)
          )
      )
  )

  query(
    Concept("Sale")
      .hasFeature(REVENUE)
      .->("geog")(
        Level("City")
          .partOf(
            Level("Region")
              .hasFeature(REGION)
          )
      )
  )

  wrapper(
    Wrapper("W1")
      .hasAttribute {
        Attribute("revenue1") sameAs REVENUE
      }
      .hasAttribute {
        Attribute("city1") sameAs CITY
      }
  )

  wrapper(
    Wrapper("W2")
      .hasAttribute {
        Attribute("revenue2") sameAs REVENUE
      }
      .hasAttribute {
        Attribute("region1") sameAs REGION
      }
  )

  wrapper(
    Wrapper("LUT1")
      .hasAttribute {
        Attribute("city2") sameAs CITY
      }
      .hasAttribute {
        Attribute("region2") sameAs REGION
      }
  )

}

object GeographicCiRe extends App {
  new GeographicCiRe().run(true)
}