![GitHub contributors](https://img.shields.io/github/contributors/pistocchifilippo/Thesis)
![GitHub last commit (branch)](https://img.shields.io/github/last-commit/pistocchifilippo/Thesis/master)
![GitHub](https://img.shields.io/github/license/pistocchifilippo/Thesis)
![GitHub release (latest by date)](https://img.shields.io/github/v/release/pistocchifilippo/Thesis)

# Implicit Roll-Up

This project is related to my master thesis "Implicit Roll-Up over graph based data integration systems" carried out in Universitat Politècnica de Catalunya - UPC upon FIB Data Science department.
This project is an extension of the project [NexitaQR](https://github.com/pistocchifilippo/NextiaQR) system, that have also been developed by the reserach group I have been working with.
In this repository you can expect to find the following:

1. An OO model able to represent any kind of graph that can be described in NextiaQR.
2. An implementation of the Implicit Roll-Up algorithm.
3. A DSL able to define a running scenario.
4. A DSL able to define an integration graph.

## Dependencies
This project have a dependency with [NexitaQR](https://github.com/pistocchifilippo/NextiaQR), that's why it will be necessary to generate and import the jar file of NexitaQR in the directory `app/NextiaQR/NextiaQR-1.0-SNAPSHOT.jar`, [here](https://github.com/pistocchifilippo/Thesis/tree/master/app).
The dependecy is already imported in `build.gradle`.

## Basic example
Will follow a basic example of a simple run of the project.
A simple DSL will facilitate the developing of a runnable execution scenario.

Target graph definition:
``` scala
val REGION = IdFeature("Region")
val COUNTRY = IdFeature("Country")
val NAME = IdFeature("Name")
val CATEGORY = IdFeature("Category")
val REVENUE = Measure("Revenue")

val multidimensionalGraph =
   Concept("Sales")
   .hasFeature {REVENUE}
   .->("location") { // Concept to concept/level relationship
      Level("Region")
      .hasFeature {REGION}
      .partOf { // Level to level relationship
         Level("Country")
         .hasFeature {COUNTRY}
      } 
   }
   .->("product") {
      Level("Name")
      .hasFeature {NAME}
      .partOf {
         Level("Category")
         .hasFeature {CATEGORY}
      } 
   }

```

Source graph definition:
``` scala
val w1 =
   Wrapper("W1")
   .hasAttribute {
      Attribute("country") sameAs COUNTRY
    }
    .hasAttribute {
      Attribute("region") sameAs REGION
    }
    .hasAttribute {
      Attribute("revenue") sameAs REVENUE
    }

```

Scenario definition:
``` scala
class ScenarioName extends Scenario {
   scenario {
      "ScenarioName"
   }
   targetGraph{
      // The target graph
   }
   wrapper {
      // One wrapper, if there are more than one repeat this block
   }
   query {
      // The query in the form of graph
   }
   aggregation {
      // One aggregation function, if there are more than one repeat this block
   } 
}

```

Run scenario:
``` scala
object ScenarioName extends App {
   new ScenarioName().run(executeImplicitRollUp = true)
}

```
## Authors
- Filippo Pistocchi
- Oscar Romero
- Sergi Nadal
