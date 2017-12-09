
import ch.systemsx.cisd.hdf5._
import ch.systemsx.cisd.hdf5.HDF5ObjectType._

object App {

  def browse (reader: IHDF5Reader, members: java.util.List[HDF5LinkInformation], prefix: String) : Unit = {
    
    for (i <- 0 until members.size()) {
      val info = members.get(i)
      val info_type = info.getType()
      val info_path = info.getPath()
      print(s"$prefix $info_path : $info_type ")
      
      info_type match {
        case DATASET    => {
          val dsInfo = reader.getDataSetInformation(info_path)
          val dsTypeInfo = dsInfo.getTypeInformation().getDataClass()
          val numElem = dsInfo.getNumberOfElements()

          dsTypeInfo match {
            case HDF5DataClass.STRING => {
              if (numElem > 1)
                println(s"number of $dsTypeInfo: $numElem. " + reader.readStringArray(info_path).mkString(", "))
              else
                println(reader.string().read(info_path))
            }
            case HDF5DataClass.FLOAT => {
              if (numElem > 1)
                println(s"number of $dsTypeInfo: $numElem. " + reader.readDoubleArray(info_path).mkString(", "))
              else
                println(reader.float64().read(info_path))
            }
            case HDF5DataClass.COMPOUND => {
              println(reader.compound().read(info_path, new HDF5CompoundDataMap().getClass()))
            }
            case _ => println(dsTypeInfo)
          }
        }
        case SOFT_LINK  => {
          println(s"$prefix    -> " + info.tryGetSymbolicLinkTarget())
        }
        case GROUP      => {
          println()
          browse(reader, 
            reader.getGroupMemberInformation(info_path, true),
            s"$prefix  ")
        }
        case _          => println("what?")
      }
    }
  }

  def main (args: Array[String]) : Unit = {
    
    if (args.length < 1)
      System.exit(1)
  
    val file_path = args(0)

    // Browse
    val reader = HDF5Factory.openForReading(file_path)
    browse(reader, reader.getGroupMemberInformation("/", true), "")
    reader.close()
  }
}
