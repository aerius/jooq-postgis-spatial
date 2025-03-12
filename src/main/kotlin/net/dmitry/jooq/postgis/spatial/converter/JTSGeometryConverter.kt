package net.dmitry.jooq.postgis.spatial.converter

import net.postgis.jdbc.PGbox2d
import net.postgis.jdbc.PGboxbase
import org.jooq.Converter
import net.postgis.jdbc.PGgeometry
import net.postgis.jdbc.geometry.GeometryCollection
import net.postgis.jdbc.geometry.LineString
import net.postgis.jdbc.geometry.LinearRing
import net.postgis.jdbc.geometry.MultiLineString
import net.postgis.jdbc.geometry.MultiPoint
import net.postgis.jdbc.geometry.MultiPolygon
import net.postgis.jdbc.geometry.Point
import net.postgis.jdbc.geometry.Polygon
import org.locationtech.jts.geom.*

/**
 * @author Dmitry Zhuravlev
 * *         Date: 07.03.16
 */
class JTSGeometryConverter : Converter<Any, Geometry> {

    private val postgisGeometryConverter = PostgisGeometryConverter()

    override fun from(obj: Any?): Geometry? = if (obj != null) toJTS(postgisGeometryConverter.from(obj)) else null

    override fun to(geom: Geometry?): Any? = if (geom != null) toNative(geom) else null

    override fun toType(): Class<Geometry> = Geometry::class.java

    override fun fromType(): Class<Any> = Any::class.java

    protected fun getGeometryFactory(srid: Int?): GeometryFactory {
        return if (srid != null) GeometryFactory(PrecisionModel(), srid) else GeometryFactory()
    }


    private fun toJTS(obj: Any?): Geometry {
        var objNotNull = obj ?: throw IllegalArgumentException("Can't convert null object to JTS Geometry")
        // in some cases, Postgis returns not PGgeometry objects
        // but org.postgis.Geometry instances.
        // This has been observed when retrieving GeometryCollections
        // as the result of an SQL-operation such as Union.
        if (objNotNull is net.postgis.jdbc.geometry.Geometry) {
            objNotNull = PGgeometry(objNotNull)
        }

        if (objNotNull is PGgeometry) {
            val out: Geometry?
            when (objNotNull.geoType) {
                net.postgis.jdbc.geometry.Geometry.POINT -> out = convertPoint(objNotNull.geometry as Point)
                net.postgis.jdbc.geometry.Geometry.LINESTRING -> out = convertLineString(
                    objNotNull.geometry as LineString)
                net.postgis.jdbc.geometry.Geometry.POLYGON -> out = convertPolygon(objNotNull.geometry as Polygon)
                net.postgis.jdbc.geometry.Geometry.MULTILINESTRING -> out = convertMultiLineString(
                    objNotNull.geometry as MultiLineString)
                net.postgis.jdbc.geometry.Geometry.MULTIPOINT -> out = convertMultiPoint(
                    objNotNull.geometry as MultiPoint)
                net.postgis.jdbc.geometry.Geometry.MULTIPOLYGON -> out = convertMultiPolygon(
                    objNotNull.geometry as MultiPolygon)
                net.postgis.jdbc.geometry.Geometry.GEOMETRYCOLLECTION -> out = convertGeometryCollection(
                    objNotNull.geometry as GeometryCollection)
                else -> throw RuntimeException("Unknown type of PGgeometry")
            }
            out.srid = objNotNull.geometry.srid
            return out
        } else if (objNotNull is PGboxbase) {
            return convertBox(objNotNull)
        } else {
            throw IllegalArgumentException(
                    "Can't convert object of type " + objNotNull.javaClass.canonicalName)

        }

    }

    private fun convertBox(box: PGboxbase): Geometry {
        val ll = box.llb
        val ur = box.urt
        val ringCoords = arrayOfNulls<Coordinate>(5)
        if (box is PGbox2d) {
            ringCoords[0] = Coordinate(ll.x, ll.y)
            ringCoords[1] = Coordinate(ur.x, ll.y)
            ringCoords[2] = Coordinate(ur.x, ur.y)
            ringCoords[3] = Coordinate(ll.x, ur.y)
            ringCoords[4] = Coordinate(ll.x, ll.y)
        } else {
            ringCoords[0] = Coordinate(ll.x, ll.y, ll.z)
            ringCoords[1] = Coordinate(ur.x, ll.y, ll.z)
            ringCoords[2] = Coordinate(ur.x, ur.y, ur.z)
            ringCoords[3] = Coordinate(ll.x, ur.y, ur.z)
            ringCoords[4] = Coordinate(ll.x, ll.y, ll.z)
        }
        val shell = getGeometryFactory(ll.srid).createLinearRing(ringCoords)
        return getGeometryFactory(ll.srid).createPolygon(shell, null)
    }

    private fun convertGeometryCollection(collection: GeometryCollection): Geometry {
        val geometries = collection.geometries
        val jtsGeometries = arrayOfNulls<Geometry>(geometries.size)
        for (i in geometries.indices) {
            jtsGeometries[i] = toJTS(geometries[i])
            //TODO  - refactor this so the following line is not necessary
            jtsGeometries[i]?.srid = 0 // convert2JTS sets SRIDs, but constituent geometries in a collection must have srid  == 0
        }
        val jtsGCollection = getGeometryFactory(collection.srid).createGeometryCollection(jtsGeometries)
        return jtsGCollection
    }

    private fun convertMultiPolygon(pgMultiPolygon: MultiPolygon): Geometry {
        val polygons = arrayOfNulls<org.locationtech.jts.geom.Polygon>(pgMultiPolygon.numPolygons())

        for (i in polygons.indices) {
            val pgPolygon = pgMultiPolygon.getPolygon(i)
            polygons[i] = convertPolygon(pgPolygon) as org.locationtech.jts.geom.Polygon
        }

        val out = getGeometryFactory(pgMultiPolygon.srid).createMultiPolygon(polygons)
        return out
    }

    private fun convertMultiPoint(pgMultiPoint: MultiPoint): Geometry {
        val points = arrayOfNulls<org.locationtech.jts.geom.Point>(pgMultiPoint.numPoints())

        for (i in points.indices) {
            points[i] = convertPoint(pgMultiPoint.getPoint(i))
        }
        val out = getGeometryFactory(pgMultiPoint.srid).createMultiPoint(points)
        out.srid = pgMultiPoint.srid
        return out
    }

    private fun convertMultiLineString(
            mlstr: MultiLineString): Geometry {
        val out: org.locationtech.jts.geom.MultiLineString
        if (mlstr.haveMeasure) {
            val lstrs = arrayOfNulls<org.locationtech.jts.geom.LineString>(mlstr.numLines())
            for (i in 0..mlstr.numLines() - 1) {
                val coordinates = toJTSCoordinates(mlstr.getLine(i).points)
                lstrs[i] = getGeometryFactory(mlstr.srid).createLineString(coordinates)
            }
            out = getGeometryFactory(mlstr.srid).createMultiLineString(lstrs)
        } else {
            val lstrs = arrayOfNulls<org.locationtech.jts.geom.LineString>(mlstr.numLines())
            for (i in 0..mlstr.numLines() - 1) {
                lstrs[i] = getGeometryFactory(mlstr.srid).createLineString(
                        toJTSCoordinates(mlstr.getLine(i).points))
            }
            out = getGeometryFactory(mlstr.srid).createMultiLineString(lstrs)
        }
        return out
    }

    private fun convertPolygon(
            polygon: Polygon): Geometry {
        val shell = getGeometryFactory(polygon.srid).createLinearRing(
                toJTSCoordinates(polygon.getRing(0).points))
        val out: org.locationtech.jts.geom.Polygon?
        if (polygon.numRings() > 1) {
            val rings = arrayOfNulls<org.locationtech.jts.geom.LinearRing>(polygon.numRings() - 1)
            for (r in 1..polygon.numRings() - 1) {
                rings[r - 1] = getGeometryFactory(polygon.srid).createLinearRing(
                        toJTSCoordinates(polygon.getRing(r).points))
            }
            out = getGeometryFactory(polygon.srid).createPolygon(shell, rings)
        } else {
            out = getGeometryFactory(polygon.srid).createPolygon(shell, null)
        }
        return out
    }

    private fun convertPoint(pnt: Point): org.locationtech.jts.geom.Point {
        val g = getGeometryFactory(pnt.srid).createPoint(
                this.toJTSCoordinate(pnt))
        return g
    }

    private fun convertLineString(
            lstr: LineString): org.locationtech.jts.geom.LineString {
        val out = if (lstr.haveMeasure)
            getGeometryFactory(lstr.srid).createLineString(toJTSCoordinates(lstr.points))
        else
            getGeometryFactory(lstr.srid).createLineString(
                    toJTSCoordinates(lstr.points))
        return out
    }

    private fun toJTSCoordinates(points: Array<Point>): Array<Coordinate?> {
        val coordinates = arrayOfNulls<Coordinate>(points.size)
        for (i in points.indices) {
            coordinates[i] = this.toJTSCoordinate(points[i])
        }
        return coordinates
    }

    private fun toJTSCoordinate(pt: Point): Coordinate {
        val mc: Coordinate = if (pt.dimension == 2) {
            if (pt.haveMeasure)
                CoordinateXYM(pt.getX(), pt.getY(), pt.getM())
            else
                Coordinate(pt.getX(), pt.getY())
        } else {
            if (pt.haveMeasure)
                CoordinateXYZM(pt.getX(), pt.getY(), pt.getZ(), pt.getM())
            else
                Coordinate(pt.getX(), pt.getY(), pt.getZ())
        }
        return mc
    }


    /**
     * Converts a JTS `Geometry` to a native geometry object.

     * @param jtsGeom    JTS Geometry to convert
     * *
     * @return native database geometry object corresponding to jtsGeom.
     */
    protected fun toNative(jtsGeom: Geometry): PGgeometry {
        val jtsGeomNotNull = forceEmptyToGeometryCollection(jtsGeom)
        val geom: net.postgis.jdbc.geometry.Geometry? = when (jtsGeomNotNull) {
            is org.locationtech.jts.geom.Point -> {
                convertJTSPoint(jtsGeomNotNull)
            }
            is org.locationtech.jts.geom.LineString -> {
                convertJTSLineString(jtsGeomNotNull)
            }
            is org.locationtech.jts.geom.MultiLineString -> {
                convertJTSMultiLineString(jtsGeomNotNull)
            }
            is org.locationtech.jts.geom.Polygon -> {
                convertJTSPolygon(jtsGeomNotNull)
            }
            is org.locationtech.jts.geom.MultiPoint -> {
                convertJTSMultiPoint(jtsGeomNotNull)
            }
            is org.locationtech.jts.geom.MultiPolygon -> {
                convertJTSMultiPolygon(jtsGeomNotNull)
            }
            is org.locationtech.jts.geom.GeometryCollection -> {
                convertJTSGeometryCollection(jtsGeomNotNull)
            }
            else -> {
                null
            }
        }

        if (geom != null) {
            return PGgeometry(geom)
        } else {
            throw UnsupportedOperationException(
                    "Conversion of "
                            + jtsGeomNotNull.javaClass.simpleName
                            + " to PGgeometry not supported")
        }
    }


    //Postgis treats every empty geometry as an empty geometrycollection

    private fun forceEmptyToGeometryCollection(jtsGeom: Geometry): Geometry {
        var forced = jtsGeom
        if (forced.isEmpty) {
            var factory: GeometryFactory? = jtsGeom.factory
            if (factory == null) {
                factory = GeometryFactory()
            }
            forced = factory?.createGeometryCollection(null)!!
            forced.setSRID(jtsGeom.srid)
        }
        return forced
    }

    private fun convertJTSMultiPolygon(
            multiPolygon: org.locationtech.jts.geom.MultiPolygon): MultiPolygon {
        val pgPolygons = arrayOfNulls<Polygon>(multiPolygon.numGeometries)
        for (i in pgPolygons.indices) {
            pgPolygons[i] = convertJTSPolygon(
                    multiPolygon.getGeometryN(i) as org.locationtech.jts.geom.Polygon)
        }
        val mpg = MultiPolygon(pgPolygons)
        mpg.setSrid(multiPolygon.srid)
        return mpg
    }

    private fun convertJTSMultiPoint(
            multiPoint: org.locationtech.jts.geom.MultiPoint): MultiPoint {
        val pgPoints = arrayOfNulls<Point>(multiPoint.numGeometries)
        for (i in pgPoints.indices) {
            pgPoints[i] = convertJTSPoint(
                    multiPoint.getGeometryN(i) as org.locationtech.jts.geom.Point)
        }
        val mp = MultiPoint(pgPoints)
        mp.setSrid(multiPoint.srid)
        return mp
    }

    private fun convertJTSPolygon(
            jtsPolygon: org.locationtech.jts.geom.Polygon): Polygon {
        val numRings = jtsPolygon.numInteriorRing
        val rings = arrayOfNulls<LinearRing>(numRings + 1)
        rings[0] = convertJTSLineStringToLinearRing(
                jtsPolygon.exteriorRing)
        for (i in 0..numRings - 1) {
            rings[i + 1] = convertJTSLineStringToLinearRing(
                    jtsPolygon.getInteriorRingN(i))
        }
        val polygon = Polygon(rings)
        polygon.setSrid(jtsPolygon.srid)
        return polygon
    }

    private fun convertJTSLineStringToLinearRing(
            lineString: org.locationtech.jts.geom.LineString): LinearRing {
        val lr = LinearRing(toPoints(lineString.coordinates))
        lr.setSrid(lineString.srid)
        return lr
    }

    private fun convertJTSLineString(string: org.locationtech.jts.geom.LineString): LineString {
        val ls = LineString(toPoints(string.coordinates))
        ls.haveMeasure = true
        ls.setSrid(string.srid)
        return ls
    }

    private fun convertJTSMultiLineString(
            string: org.locationtech.jts.geom.MultiLineString): MultiLineString {
        val lines = arrayOfNulls<LineString>(string.numGeometries)
        for (i in 0..string.numGeometries - 1) {
            lines[i] = LineString(toPoints(string.getGeometryN(i).coordinates))
        }
        val mls = MultiLineString(lines)
        mls.haveMeasure = true
        mls.setSrid(string.srid)
        return mls
    }

    private fun convertJTSPoint(point: org.locationtech.jts.geom.Point): Point {
        val pgPoint = Point()
        pgPoint.srid = point.srid
        pgPoint.x = point.x
        pgPoint.y = point.y
        val coordinate = point.coordinate
        if (java.lang.Double.isNaN(coordinate.z)) {
            pgPoint.dimension = 2
        } else {
            pgPoint.z = coordinate.z
            pgPoint.dimension = 3
        }
        pgPoint.haveMeasure = false
        if (coordinate is Coordinate && !java.lang.Double.isNaN(coordinate.m)) {
            pgPoint.m = coordinate.m
            pgPoint.haveMeasure = true
        }
        return pgPoint
    }

    private fun convertJTSGeometryCollection(
            collection: org.locationtech.jts.geom.GeometryCollection): GeometryCollection {
        var currentGeom: Geometry
        val pgCollections = arrayOfNulls<net.postgis.jdbc.geometry.Geometry>(collection.numGeometries)
        for (i in pgCollections.indices) {
            currentGeom = collection.getGeometryN(i)
            currentGeom = forceEmptyToGeometryCollection(currentGeom)
            when (currentGeom.javaClass) {
                org.locationtech.jts.geom.LineString::class.java -> {
                    pgCollections[i] = convertJTSLineString(currentGeom as org.locationtech.jts.geom.LineString)
                }
                org.locationtech.jts.geom.LinearRing::class.java -> {
                    pgCollections[i] = convertJTSLineStringToLinearRing(currentGeom as org.locationtech.jts.geom.LinearRing)
                }
                org.locationtech.jts.geom.MultiLineString::class.java -> {
                    pgCollections[i] = convertJTSMultiLineString(currentGeom as org.locationtech.jts.geom.MultiLineString)
                }
                org.locationtech.jts.geom.MultiPoint::class.java -> {
                    pgCollections[i] = convertJTSMultiPoint(currentGeom as org.locationtech.jts.geom.MultiPoint)
                }
                org.locationtech.jts.geom.MultiPolygon::class.java -> {
                    pgCollections[i] = convertJTSMultiPolygon(currentGeom as org.locationtech.jts.geom.MultiPolygon)
                }
                org.locationtech.jts.geom.Point::class.java -> {
                    pgCollections[i] = convertJTSPoint(currentGeom as org.locationtech.jts.geom.Point)
                }
                org.locationtech.jts.geom.Polygon::class.java -> {
                    pgCollections[i] = convertJTSPolygon(currentGeom as org.locationtech.jts.geom.Polygon)
                }
                org.locationtech.jts.geom.GeometryCollection::class.java -> {
                    pgCollections[i] = convertJTSGeometryCollection(currentGeom as org.locationtech.jts.geom.GeometryCollection)
                }
            }
        }
        val gc = GeometryCollection(pgCollections)
        gc.setSrid(collection.srid)
        return gc
    }


    private fun toPoints(coordinates: Array<Coordinate>): Array<Point?> {
        val points = arrayOfNulls<Point>(coordinates.size)
        for (i in coordinates.indices) {
            val c = coordinates[i]
            val pt: Point
            if (java.lang.Double.isNaN(c.z)) {
                pt = Point(c.x, c.y)
            } else {
                pt = Point(c.x, c.y, c.z)
            }
            if (c is Coordinate) {
                val mc = c
                if (!java.lang.Double.isNaN(mc.m)) {
                    pt.setM(mc.m)
                }
            }
            points[i] = pt
        }
        return points
    }


}
