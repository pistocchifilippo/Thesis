package implicitAggregation.scenarios.sales

import implicitAggregation.dsl.Scenario
import implicitAggregation.model.{AggregatingFunction, Attribute, Concept, IdFeature, Level, Measure, Wrapper}

class GeographicCiReCoCo extends Scenario {

  scenario{
    "GeographicCiReCoCo"
  }

  val CITY = IdFeature("City_f")
  val REGION = IdFeature("Region_f")
  val COUNTRY = IdFeature("Country_f")
  val CONTINENT = IdFeature("Continent_f")

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
              .partOf(
                Level("Country")
                  .hasFeature(COUNTRY)
                  .partOf(
                    Level("Continent")
                      .hasFeature(CONTINENT)
                  )
              )
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
              .partOf(
                Level("Country")
                  .partOf(
                    Level("Continent")
                      .hasFeature(CONTINENT)
                  )

              )
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
    Wrapper("W3")
      .hasAttribute {
        Attribute("revenue3") sameAs REVENUE
      }
      .hasAttribute {
        Attribute("country1") sameAs COUNTRY
      }
  )

  wrapper(
    Wrapper("W4")
      .hasAttribute {
        Attribute("revenue4") sameAs REVENUE
      }
      .hasAttribute {
        Attribute("continent1") sameAs CONTINENT
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

  wrapper(
    Wrapper("LUT2")
      .hasAttribute {
        Attribute("region3") sameAs REGION
      }
      .hasAttribute {
        Attribute("country2") sameAs COUNTRY
      }
  )

  wrapper(
    Wrapper("LUT3")
      .hasAttribute {
        Attribute("country3") sameAs COUNTRY
      }
      .hasAttribute {
        Attribute("continent2") sameAs CONTINENT
      }
  )

  aggregation(
    AggregatingFunction("avg") aggregates REVENUE
  )

}

object GeographicCiReCoCo extends App {
  new GeographicCiReCoCo().run(true)
}
