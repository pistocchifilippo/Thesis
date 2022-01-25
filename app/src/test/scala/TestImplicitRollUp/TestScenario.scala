package TestImplicitRollUp

import implicitAggregation.model.{Concept, IdFeature, Level, Measure}

object TestScenario {

  val M = Measure("M")
  val ID_L11 = IdFeature("ID_L11")
  val ID_L21 = IdFeature("ID_L21")

  val targetGraph =
  Concept("X")
    .hasFeature(M)
    .hasConcept("D1")(
      Level("L11")
        .hasFeature(ID_L11)
    )
    .hasConcept("D2")(
      Level("L21")
        .hasFeature(ID_L21)
    )


}
