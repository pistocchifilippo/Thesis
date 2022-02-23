package implicitAggregation.scenarios.sales

import implicitAggregation.dsl.Scenario
import implicitAggregation.model.{AggregatingFunction, Attribute, Concept, IdFeature, Level, Measure, Wrapper}

class GeogProdTimeCiRePrCaDaMo extends Scenario {

  scenario{
    "GeogProdTimeCiRePrCaDaMo"
  }

  val CITY = IdFeature("City_f")
  val REGION = IdFeature("Region_f")
  val PRODUCT = IdFeature("Product_f")
  val CATEGORY = IdFeature("Category_f")
  val DAY = IdFeature("Day_f")
  val MONTH = IdFeature("Month_f")

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
      .->("time")(
        Level("Day")
          .hasFeature(DAY)
          .partOf(
            Level("Month")
              .hasFeature(MONTH)
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
      .->("time")(
        Level("Day")
          .partOf(
            Level("Month")
              .hasFeature(MONTH)
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
      .hasAttribute {
        Attribute("month1") sameAs MONTH
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
      .hasAttribute {
        Attribute("day2") sameAs DAY
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
      .hasAttribute {
        Attribute("month3") sameAs MONTH
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
      .hasAttribute {
        Attribute("month4") sameAs MONTH
      }
  )

  wrapper(
    Wrapper("LUT5")
      .hasAttribute {
        Attribute("city_lut_5") sameAs CITY
      }
      .hasAttribute {
        Attribute("region_lut_5") sameAs REGION
      }
  )

  wrapper(
    Wrapper("LUT6")
      .hasAttribute {
        Attribute("product_lut_6") sameAs PRODUCT
      }
      .hasAttribute {
        Attribute("category_lut_6") sameAs CATEGORY
      }
  )

  wrapper(
    Wrapper("LUT8")
      .hasAttribute {
        Attribute("day_lut_8") sameAs DAY
      }
      .hasAttribute {
        Attribute("month_lut_8") sameAs MONTH
      }
  )

  aggregation(
    AggregatingFunction("sum") aggregates REVENUE
  )

}

object GeogProdTimeCiRePrCaDaMo extends App {
  new GeogProdTimeCiRePrCaDaMo().run(true)
}


