package implicitAggregation.scenarios

import implicitAggregation.dsl.Scenario
import implicitAggregation.model.{Attribute, Concept, Fact, GenericFeature, IdFeature, Wrapper}

class TestNewAlgorithm extends Scenario{
  scenario{
    "TestNewAlgorithm"
  }
  val NAME = GenericFeature("Name")
  val PID = IdFeature("Pid")
  val DID = IdFeature("Did")
  val DEPARTMENT = GenericFeature("Department")

  targetGraph{
    Fact("Person")
      .hasFeature(NAME)
      .hasFeature(PID)
      .->("worksIn"){
        Concept("Department")
          .hasFeature(DID)
          .hasFeature(DEPARTMENT)
      }
  }

  wrapper(
    Wrapper("W1")
      .hasAttribute {
        Attribute("dep") sameAs DEPARTMENT
      }
      .hasAttribute {
        Attribute("did") sameAs DID
      }
  )

  wrapper(
    Wrapper("W2")
      .hasAttribute {
        Attribute("name") sameAs NAME
      }
      .hasAttribute {
        Attribute("did2") sameAs DID
      }
      .hasAttribute {
        Attribute("pid") sameAs PID
      }
  )
}

object TestNewAlgorithm extends App {
  new TestNewAlgorithm().run(false)
}
