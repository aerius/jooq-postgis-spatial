package net.dmitry.jooq.postgis.spatial.converter

import org.locationtech.jts.geom.Coordinate
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.locationtech.jts.geom.CoordinateXYM
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.postgresql.util.PGobject

/**
 * @author Dmitry Zhuravlev
 * *         Date: 08.03.16
 */
class JTSGeometryConverterTest {

    val jtsGeometryConverter = JTSGeometryConverter()

    @Test
    fun testFromGeography() {
        val pGobject = PGobject().apply {
            type = "geography"
            value = "0101000020E6100000304CA60A460D4140BE9F1A2FDD0C4E40"
        }
        val converted = jtsGeometryConverter.from(pGobject)
        assertTrue(converted is Geometry)
        assertEquals("Point", converted!!.geometryType)
        assertEquals(34.1037, converted.coordinate.x , 0.0001)
        assertEquals(60.1005, converted.coordinate.y, 0.0001)
    }

    @Test
    fun testFromGeometry() {
        val pGobject = PGobject().apply {
            type = "geometry"
            value = "0101000020E6100000304CA60A460D4140BE9F1A2FDD0C4E40"
        }
        val converted = jtsGeometryConverter.from(pGobject)
        assertTrue(converted is Geometry)
        assertEquals("Point", converted!!.geometryType)
        assertEquals(34.1037, converted.coordinate.x, 0.0001)
        assertEquals(60.1005, converted.coordinate.y, 0.0001)
    }

    @Test
    fun testTo() {
        val x = 34.1037
        val y = 60.1005
        val jtsPoint = GeometryFactory().createPoint(Coordinate(x, y))
        val convertedBack = jtsGeometryConverter.to(jtsPoint)
        assertTrue(convertedBack is net.postgis.jdbc.PGgeometry
                && convertedBack.geometry.getPoint(0).x == x && convertedBack.geometry.getPoint(0).y == y)
    }

    @Test
    fun testToWithMeasure() {
        val x = 34.1037
        val y = 60.1005
        val m = 100.0
        val c = CoordinateXYM(x, y, m)

        val jtsPoint = GeometryFactory().createPoint(c)
        val convertedBack = jtsGeometryConverter.to(jtsPoint)
        assertTrue(convertedBack is net.postgis.jdbc.PGgeometry
                && convertedBack.geometry.getPoint(0).x == x && convertedBack.geometry.getPoint(0).y == y && convertedBack.geometry.getPoint(0).m == m)
    }
}