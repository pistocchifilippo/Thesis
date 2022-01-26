package TestImplicitRollUp


import org.scalatest.funsuite.AnyFunSuite
import implicitAggregation.model.ImplicitRollUp._

class Test extends AnyFunSuite {

  test("An empty Set should have size 0") {
    assert(allDimensions(TestScenario.targetGraph).size == 2)
  }

  //  test("Invoking head on an empty Set should produce NoSuchElementException") {
  //    assertThrows[NoSuchElementException] {
  //      Set.empty.head
  //    }
  //  }
}
