package implicitAggregation.scenarios

import implicitAggregation.dsl.Scenario
import implicitAggregation.model._

class Correctness extends Scenario {
  scenario{
    "Correctness"
  }

  val CITY = IdFeature("City")
  val SALE_ID = IdFeature("Id")
  val PRODUCT = GenericFeature("Product")
  val UNIT_SOLD = GenericFeature("UnitSold")

  targetGraph{
    Concept("Sale")
      .hasFeature{CITY}
      .hasFeature{SALE_ID}
      .hasFeature{PRODUCT}
      .hasFeature{UNIT_SOLD}
  }

  wrapper{
    Wrapper("W1")
      .hasAttribute {
        Attribute("city") sameAs CITY
      }
      .hasAttribute {
        Attribute("id1") sameAs SALE_ID
      }
      .hasAttribute {
        Attribute("unit_sold") sameAs UNIT_SOLD
      }
  }

  wrapper{
    Wrapper("W2")
      .hasAttribute {
        Attribute("id2") sameAs SALE_ID
      }
      .hasAttribute {
        Attribute("city1") sameAs CITY
      }
      .hasAttribute {
        Attribute("product") sameAs PRODUCT
      }
  }

}

object Correctness extends App {
  new Correctness().run(false)
}
