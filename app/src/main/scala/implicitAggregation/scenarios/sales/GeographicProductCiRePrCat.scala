package implicitAggregation.scenarios.sales

import implicitAggregation.dsl.Scenario
import implicitAggregation.model.{AggregatingFunction, Attribute, Concept, IdFeature, Level, Measure, Wrapper}

class GeographicProductCiRePrCat extends Scenario {

  scenario{
    "GeographicProductCiRePrCat"
  }

  val CITY = IdFeature("City_f")
  val REGION = IdFeature("Region_f")
  val PRODUCT = IdFeature("Product_f")
  val CATEGORY = IdFeature("Category_f")

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
      .->("prod")(
        Level("Product")
          .hasFeature(PRODUCT)
          .partOf(
            Level("Category")
              .hasFeature(CATEGORY)
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
      .->("prod")(
        Level("Product")
          .partOf(
            Level("Category")
              .hasFeature(CATEGORY)
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
      .hasAttribute {
        Attribute("category1") sameAs CATEGORY
      }
  )

  wrapper(
    Wrapper("W2")
      .hasAttribute {
        Attribute("revenue2") sameAs REVENUE
      }
      .hasAttribute {
        Attribute("region2") sameAs REGION
      }
      .hasAttribute {
        Attribute("product2") sameAs PRODUCT
      }
  )

  wrapper(
    Wrapper("W3")
      .hasAttribute {
        Attribute("revenue3") sameAs REVENUE
      }
      .hasAttribute {
        Attribute("region3") sameAs REGION
      }
      .hasAttribute {
        Attribute("category3") sameAs CATEGORY
      }
  )

  wrapper(
    Wrapper("W4")
      .hasAttribute {
        Attribute("revenue4") sameAs REVENUE
      }
      .hasAttribute {
        Attribute("city4") sameAs CITY
      }
      .hasAttribute {
        Attribute("product4") sameAs PRODUCT
      }
  )

  wrapper(
    Wrapper("W7")
      .hasAttribute {
        Attribute("revenue7") sameAs REVENUE
      }
      .hasAttribute {
        Attribute("city7") sameAs CITY
      }
      .hasAttribute {
        Attribute("product7") sameAs PRODUCT
      }
  )

  wrapper(
    Wrapper("LUT5")
      .hasAttribute {
        Attribute("city5") sameAs CITY
      }
      .hasAttribute {
        Attribute("region5") sameAs REGION
      }
  )

  wrapper(
    Wrapper("LUT6")
      .hasAttribute {
        Attribute("product6") sameAs PRODUCT
      }
      .hasAttribute {
        Attribute("category6") sameAs CATEGORY
      }
  )

  aggregation(
    AggregatingFunction("avg") aggregates REVENUE
  )

}

object GeographicProductCiRePrCat extends App {
  new GeographicProductCiRePrCat().run(true)
}

