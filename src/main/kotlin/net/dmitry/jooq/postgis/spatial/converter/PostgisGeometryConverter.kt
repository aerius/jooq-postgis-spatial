package net.dmitry.jooq.postgis.spatial.converter

import net.postgis.jdbc.geometry.Geometry
import net.postgis.jdbc.geometry.GeometryBuilder
import org.postgresql.util.PGobject
import org.jooq.Converter

/**
 * @author Dmitry Zhuravlev
 *         Date: 07.03.16
 */
class PostgisGeometryConverter : Converter<Any, Geometry> {

    override fun from(obj: Any): Geometry = GeometryBuilder.geomFromString(obj.toString())


    override fun to(geom: Geometry): Any = PGobject().apply {
                    type = geom.typeString
                    value = geom.value
                }

    override fun toType(): Class<Geometry> = Geometry::class.java

    override fun fromType(): Class<Any> = Any::class.java
}