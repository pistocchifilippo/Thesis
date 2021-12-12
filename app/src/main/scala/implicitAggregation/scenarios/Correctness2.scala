package implicitAggregation.scenarios

import implicitAggregation.dsl.Scenario
import implicitAggregation.model.{Attribute, Concept, GenericFeature, IdFeature, Wrapper}

class Correctness2 extends Scenario{
  scenario(
    "Correctness2"
  )
  val CITY = GenericFeature("City")
  val SALE_ID = IdFeature("Id")
  val PRODUCT = GenericFeature("Product")

  targetGraph{
    Concept("Sale")
      .hasFeature{CITY}
      .hasFeature{SALE_ID}
      .hasFeature{PRODUCT}
  }

  wrapper{
    Wrapper("W1")
      .hasAttribute {
        Attribute("city") sameAs CITY
      }
      .hasAttribute {
        Attribute("id") sameAs SALE_ID
      }
  }

  wrapper{
    Wrapper("W2")
      .hasAttribute {
        Attribute("id2") sameAs SALE_ID
      }
      .hasAttribute {
        Attribute("prod") sameAs PRODUCT
      }
  }

}

object Correctness2 extends App{
  new Correctness2().run(false)
}
