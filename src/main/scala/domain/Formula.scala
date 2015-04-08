package domain

import domain.Clause
import utils.SatLoggingUtils
import utils.SatReader._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem, FSDataInputStream}
import org.apache.log4j.Logger
import scala.collection.immutable.HashMap
import scala.io.Source

/**
 * Created by marbarfa on 3/2/14.
 */
class Formula extends SatLoggingUtils {

  var clauses: List[Clause] = List[Clause]()
  var clausesOfVars: Map[Int, List[Clause]] = new HashMap[Int, List[Clause]]()

  var n: Int = _
  var m: Int = _

  var literalsInOrder: List[Int] = null;


  /**
   * @return true if the formula is satisfiable
   */
  def isSatisfasiable(literals: List[Int], log: Logger): Boolean = {
    var res = true
    var affectedClauses = getClauses(literals, log)
    for (c <- affectedClauses if res) {
      res = res && c.isSatisfasiable(literals)
    }
    return res
  }

  def getClauses(literals: List[Int], log: Logger): List[Clause] = {
    var clauses = literals.foldLeft(List[Clause]()) { (list, key) => list ::: clausesOfVars.getOrElse(math.abs(key), List[Clause]()) }
    return clauses;
  }


  /**
   * Adds a new dependable clause to a literal @literal.
   * This method is used to have a map of literal ==> where it appears in the Formula.
   * @param literal
   * @param clause
   */
  def addClauseOfVar(literal: Int, clause: Clause) {
    var literalAbs = math.abs(literal);
    clausesOfVars += (literalAbs -> (clause :: clausesOfVars.getOrElse(literalAbs, List[Clause]())))
  }

  def getFalseClauses(literals: List[Int]): List[Clause] = {
    var falseClauses = List[Clause]();

    literals.foreach(literal => {
      clausesOfVars
        .getOrElse(math.abs(literal), List[Clause]())
        .foreach(clause => {
        if (!falseClauses.contains(clause) &&
          !clause.isSatisfasiable(literals)) {
          falseClauses ::= clause
        }
      })
    })

    return falseClauses;
  }

  /**
   * Order clausesOfVars by how many clauses are related to each literal
   * @return
   */
  def getLiteralsInOrder(): List[Int] = {
    if (literalsInOrder == null) {
      literalsInOrder = clausesOfVars.toSeq.sortBy(_._2.size).reverse.toList map (x => x._1)
      log.info(s"Literals in order: ${literalsInOrder}")
    }
    return literalsInOrder;
  }

  /**
   * Returns a CNF formatted 3SAT formula
   * @return
   */
  def toCNF: String = {
    val builder = new StringBuilder()
    builder.append(s"p $n $m\n")
    for (clause <- clauses) {
      var clauseStr = ""
      for (literal <- clause.literals) {
        clauseStr += s" $literal"
      }
      builder append clauseStr.trim
      builder append "\n"
    }
    builder append "%"
    return builder.toString()
  }

  /**
   * Reads a formula in CNF form from an InputStream
   */
  def fromCNF(dataInputStream: FSDataInputStream) = {
    var clauseIndex: Int = 0;
    var end = false;
    for (line <- Source.fromInputStream(dataInputStream).getLines() if !end) {
      if (line.startsWith("%")) {
        end = true
      } else {
        //ignore commented lines => starting with # or with the character 'c'
        if (!line.startsWith("#") && !line.startsWith("c") && !line.isEmpty) {
          if (line.startsWith("p")) {
            //it has a problem definition => initialize.
            val problemDef: Array[String] = line.split(" ")
            for (s <- problemDef if m * n == 0) {
              try {
                val intVal = s.trim().toInt
                if (n == 0) n = intVal else m = intVal
              } catch {
                case e: Any => //ignore
              }
            }
          } else {
            var clause = new Clause
            //read each var of the current clause.
            line.trim().split(" ").foreach(v => {
              val readVar = v.toInt
              if (readVar != 0) {
                // ignore 0 literals => should be the last one
                clause.literals ::= readVar
                addClauseOfVar(readVar, clause);
              }
            })
            if (clause.literals.size > 0) {
              clause.id = clauseIndex;
              clauses ::= clause
              clauseIndex += 1
            }
          }
        }
      }
    }

  }

}
