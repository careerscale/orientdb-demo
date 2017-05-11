package com.orientdb.samples.test;

import java.io.IOException;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.metadata.sequence.OSequence;

public class OrientDbTest {


    @Test
    public void testOrientDB() throws IOException {


        OrientGraphFactory factory =
                new OrientGraphFactory("remote:localhost/mlm", "root", "password").setupPool(1, 10);

        OrientGraph db = factory.getTx();
        try {
            OSequence seq = db.getRawDatabase().getMetadata().getSequenceLibrary().getSequence("companyIdSequence");
            Vertex v = db.addVertex("Person");
            v.properties("id", "" + seq.next());

            // db.commit();
            db.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.close();
            factory.close();
        }
    }


}


