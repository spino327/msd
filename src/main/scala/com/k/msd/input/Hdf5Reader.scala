
package com.k.msd.input

import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.collection.JavaConverters._

import ch.systemsx.cisd.hdf5._
import ch.systemsx.cisd.hdf5.HDF5ObjectType._

class HDF5Obj (m:Map[String, Any]) {
  // primary constructor
  require(m != null)
  private val map = m

  // member methods
  /**
    * Retrieves the value which is associated with the given key.
    */
  def apply[T](key: String): T = {
    return map(key).asInstanceOf[T]
  }

  override def toString = {
    map.map(kv => kv._1 + "[" + kv._2.getClass() + "]: " + kv._2).mkString("\n")
  }

}

object HDF5Reader {

  // member methods
  def process(file_path:String) : HDF5Obj = {

    val reader = HDF5Factory.openForReading(file_path)
    val map = new HashMap[String, Any]() //.++(paths) 

    browse(reader, map, reader.getGroupMemberInformation("/", true))

    reader.close()
    
    return new HDF5Obj(map)
  }

  /**
   * DFS traversing of the HDF5 starting from parent
   */
  private def browse (reader: IHDF5Reader, kvPairs: Map[String, Any], members: java.util.List[HDF5LinkInformation]) : Unit = {
    
    for (i <- 0 until members.size()) {
      val info = members.get(i)
      val info_type = info.getType()
      val info_path = info.getPath()
     
      info_type match {
        case DATASET    => {
          val dsInfo = reader.getDataSetInformation(info_path)
          val dsTypeInfo = dsInfo.getTypeInformation().getDataClass()
          val numElem = dsInfo.getNumberOfElements()

          dsTypeInfo match {
            case HDF5DataClass.STRING => {
              if (numElem > 1)
                kvPairs(info_path) = reader.readStringArray(info_path)
              else
                kvPairs(info_path) = reader.string().read(info_path)
            }
            case HDF5DataClass.FLOAT => {
              if (numElem > 1)
                kvPairs(info_path) = reader.readDoubleArray(info_path)
              else
                kvPairs(info_path) = reader.float64().read(info_path)
            }
            case HDF5DataClass.INTEGER => {
              if (numElem > 1)
                kvPairs(info_path) = reader.readIntArray(info_path)
              else
                kvPairs(info_path) = reader.int32().read(info_path)
            }
            case HDF5DataClass.COMPOUND => {
              for(kv <- reader.compound().read(info_path, new HDF5CompoundDataMap().getClass()).asScala) {
                kvPairs(info_path + "/" + kv._1) = kv._2 
              }
            }
            case _ => println("ERROR: Not processed: " + dsTypeInfo)
          }
        }
        case GROUP => {
          browse(reader, kvPairs, reader.getGroupMemberInformation(info_path, true))
        }
        case _ => {}
      }
    }
  }
}

