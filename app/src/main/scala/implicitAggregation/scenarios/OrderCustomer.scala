package implicitAggregation.scenarios

import implicitAggregation.model._

object OrderCustomer extends App {

  val configurationFilePath = "/Users/filippo/ImplicitAggregation/app/src/main/resources/scenarios/OrderCustomer/"
//  val configurationFilePath = "/Users/filippo/GraphBuilder/scenarios/OrderCustomer/"
  Utils.buildPath(configurationFilePath)

  // GRAPH DEFINITION

  // Features
  val DATE = GenericFeature("DATE")
  val O_ID = IdFeature("O_ID")
  val C_ID = IdFeature("C_ID")
  val NAME = GenericFeature("NAME")

  // Graph hierarchy
  val Order =
    Concept("Order")
      .->(DATE)
      .->(O_ID)
      .->("doneBy") {
        Concept("Customer")
          .->(C_ID)
          .->(NAME)
      }

  val Order2 =
    Concept("Order")
      .hasFeature{DATE}
      .hasFeature{O_ID}
      .->("doneBy"){
        Concept("Customer")
          .hasFeature{C_ID}
          .hasFeature{NAME}
      }

  // Wrappers
  val w1 =
    Wrapper("W1")
      .->(Attribute("date") sameAs DATE)
      .->(Attribute("oId") sameAs O_ID)
      .->(Attribute("cId") sameAs C_ID)

  val w2 =
    Wrapper("W2")
      .hasAttribute{Attribute("cId2") sameAs C_ID}
      .hasAttribute{Attribute("name") sameAs NAME}

  // WRITERS
  Utils.generateAllFiles(Set(Order),Set(w1,w2),Order)(configurationFilePath)
}