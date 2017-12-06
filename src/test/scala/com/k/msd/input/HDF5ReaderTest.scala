package com.k.msd.input

import org.junit.Test
import org.junit.Assert._

class HDF5ReaderTest {

  @Test
  def testReadTrackId = {
    println("testReadTrackId")
    
    val mapExp = Map('track_id -> "TRAAAAW128F429D538",
      'artist_terms -> Array("hip hop", "underground rap", "g funk", "alternative rap",
        "gothic rock", "west coast rap", "rap", "club dance",
        "singer-songwriter", "chill-out", "underground hip hop",
        "rock", "gothic", "san francisco bay area", "indie",
        "american", "punk", "california", "industrial", "new york",
        "90s", "latin", "spanish", "dark", "ebm", "underground",
        "deathrock", "west coast", "san francisco", "producer",
        "oakland", "catalan", "barcelona", "doomsdope", "norcal",
        "west coast hip hop", "alternative rock"),
      'duration -> 218.93179)
      
    val h5file = "src/test/resources/sample.h5"
    
    // reads all the data of the hdf5 file
    val current = HDF5Reader.process(h5file)
   
    println(current)

    assertEquals(mapExp('track_id), current[String]("/analysis/songs/track_id"))
    assertArrayEquals(mapExp('artist_terms).asInstanceOf[Array[Object]], current[Array[Object]]("/metadata/artist_terms"))
    assertEquals(mapExp('duration), current("/analysis/songs/duration"))
  }

  @Test
  def testReadSpecificPaths = {
    println("testReadSpecificPaths")
    val specificPaths = List("/analysis/songs/track_id", "/metadata/songs/artist_name", "/metadata/songs/artist_id", "/metadata/songs/title")

    val mapExp = Map ('artist_name -> "Casual",
      'track_id -> "TRAAAAW128F429D538",
      'artist_id -> "ARD7TVE1187B99BFB1",
      'title -> "I Didn't Mean To")

    val h5file = "src/test/resources/sample.h5"
    
    // reads just the specified paths of the hdf5 file
    val current = HDF5Reader.process(h5file, specificPaths)

    println(current)

    assertEquals(mapExp('track_id), current[String]("/analysis/songs/track_id"))
    assertEquals(mapExp('artist_name), current[String]("/metadata/songs/artist_name"))
    assertEquals(mapExp('artist_id), current[String]("/metadata/songs/artist_id"))
    assertEquals(mapExp('title), current[String]("/metadata/songs/title"))
  }

  @Test
  def testNegative = {
    println("testNegative")
    
    val h5file = "src/test/resources/sample.h5"
    val current = HDF5Reader.process(h5file)
   
    try {
      current("/not/will/be/found")
      fail("Should raise an exception")
    } catch {
      case _ : Throwable => {}
    }
  }
}
