package implicitAggregation.scenarios.sales

import implicitAggregation.dsl.Scenario
import implicitAggregation.model._

class Test extends Scenario {

  scenario{
    "test"
  }

  val L1 = IdFeature("D1L1")
  val L2 = IdFeature("D1L2")
  val M = Measure("M")

  targetGraph(
    Concept("EXPERIMENT")
      .hasFeature(M)
      .->("D1")(
        Level("D1L1")
          .hasFeature(L1)
          .partOf(
            Level("D1L2")
              .hasFeature(L2)
          )
      )
  )

  query(
    Concept("EXPERIMENT")
      .hasFeature(M)
      .->("D1")(
        Level("D1L1")
          .partOf(
            Level("D1L2")
              .hasFeature(L2)
          )
      )
  )

  wrapper(
    Wrapper("W1")
      .hasAttribute {
        Attribute("M1") sameAs M
      }
      .hasAttribute {
        Attribute("D1L11") sameAs L1
      }
  )

  wrapper(
    Wrapper("W2")
      .hasAttribute {
        Attribute("M2") sameAs M
      }
      .hasAttribute {
        Attribute("D1L22") sameAs L2
      }
  )

  wrapper(
    Wrapper("LUT3")
      .hasAttribute {
        Attribute("D1L13") sameAs L1
      }
      .hasAttribute {
        Attribute("D1L23") sameAs L2
      }
  )
}

object Test extends App {
  new Test().run(true)
}
