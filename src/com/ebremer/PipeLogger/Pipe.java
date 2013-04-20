package com.ebremer.PipeLogger;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.shared.Lock;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RiotException;
import org.apache.log4j.Logger;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.AuthenticationStore;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.BasicAuthentication;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.client.util.DigestAuthentication;
import org.eclipse.jetty.http.HttpMethod;

/**
 * Created by Erich Bremer
 * http://www.ebremer.com/paladin/pipelogger
 * Version 1.0 Beta
 */
public class Pipe {
    Model buffer = ModelFactory.createDefaultModel();
    HttpClient httpClient = new HttpClient();
    String quadstore = "http://myquadstore.com/sparql-graph-crud?graph-uri=http://the.uri.of.the.named.graph.you.are.uploadingto.com";
    URI uri = null;
    int buffersize = 0;
    int maxbuffersize = 0;
    String prefix = "@prefix http: <http://www.w3.org/2011/http#> . @prefix time: <http://www.w3.org/2006/time/> . @prefix : <http://www.ebremer.com/ont/sno#> . ";
    Timer timer;
    int flushcycle = 5000;
    boolean flush;
    Logger logger;
    String realm = "realm";
    String logpath = "";
    String user = "user";
    String password = "password";
    String me = "<localhost>";
    String authtype = "none";
    String namedgraph = "http://www.ebremer.com/log";  // this value not used yet.  It is currently part of the quadstore string as the "graph-uri" patameter
    
    public Pipe(String configfile) {
        Model config = RDFDataMgr.loadModel(configfile);
        String qs = "prefix sno: <http://www.ebremer.com/ont/sno#> prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> select ?uri ?authtype ?realm ?buffersize ?maxbuffersize ?logpath ?quadstore ?uid ?password ?flushcycle ?prefix where {?uri a sno:PipeLogger . optional{?uri sno:buffersize ?buffersize; sno:namedgraph ?namedgraph; sno:maxbuffersize ?maxbuffersize; sno:logpath ?logpath; sno:quadstore ?quadstore; sno:uid ?uid; sno:password ?password; sno:flushcycle ?flushcycle; sno:prefix ?prefix; sno:realm ?realm; sno:authtype ?authtype}} limit 1";
        Query query = QueryFactory.create(qs);
        QueryExecution qe = QueryExecutionFactory.create(query, config);
        ResultSet results = qe.execSelect();
        if (results.hasNext()) {
            QuerySolution soln = results.nextSolution();
            RDFNode n = soln.get("logpath");
            if (n != null) {
                logpath = n.asLiteral().getString();
            }
            n = soln.get("uri");
            if (n != null) {
                me = n.asResource().getURI();
            }
            n = soln.get("namedgraph");
            if (n != null) {
                namedgraph = n.asResource().getURI();
            }
            n = soln.get("authtype");
            if (n != null) {
                authtype = n.asResource().getURI();
                if (authtype.matches("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil")) {
                    authtype = "none";
                } else if (authtype.matches("http://www.ebremer.com/ont/sno#Digest")) {
                    authtype = "digest";
                } else if (authtype.matches("http://www.ebremer.com/ont/sno#Basic")) {
                    authtype = "basic";
                } else {
                    logger.error("Invalid authentication specified in configuration file...defaulting to none...");
                }
            }
            n = soln.get("realm");
            if (n != null) {
                realm = n.asLiteral().toString();
            }
            n = soln.get("buffersize");
            if (n != null) {
                buffersize = n.asLiteral().getInt();
            }
            n = soln.get("maxbuffersize");
            if (n != null) {
                maxbuffersize = n.asLiteral().getInt();
            }
            n = soln.get("flushcycle");
            if (n != null) {
                flushcycle = n.asLiteral().getInt();
            }
            n = soln.get("uid");
            if (n != null) {
                user = n.asLiteral().getString();
            }
            n = soln.get("password");
            if (n != null) {
                password = n.asLiteral().getString();
            }
            n = soln.get("quadstore");
            if (n != null) {
                quadstore = n.asLiteral().getString();
            }
            n = soln.get("prefix");
            if (n != null) {
                prefix = n.asLiteral().getString();
            }
        }
        this.flush = true;
        logger = Logger.getLogger("PipeLogger");
        try {
            uri = new URI(quadstore);
        } catch (URISyntaxException ex) {
            logger.error(ex.toString());
        }          
        httpClient.setFollowRedirects(false);
        try {
            httpClient.start();
        } catch (Exception ex) {
            logger.error(ex.toString());
        }
        AuthenticationStore auth = httpClient.getAuthenticationStore();
        if (authtype.matches("digest")) {
            auth.addAuthentication(new DigestAuthentication(uri, realm, user, password));
        } else if (authtype.matches("basic")) {
            auth.addAuthentication(new BasicAuthentication(uri, realm, user, password));
        }
        // need to add WebID support at some point
        timer = new Timer();
        timer.schedule(new flushtask(), 0, flushcycle);
    }
    
    public static void main(String[] args) {
        Log.setLog4j();
        Pipe p;
        if (args.length == 1) {
            p = new Pipe(args[0]);
        } else {
            p = new Pipe("./config.ttl");
        }
        p.pipescanner();
        System.exit(0);
    }

    public void pipescanner() {
        logger.info("Starting pipescanner...");
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            String s;
            while ((s = in.readLine()) != null && s.length() != 0) {
                InputStream is;
                try {
                    s=prefix+s;
                    is = new ByteArrayInputStream(s.getBytes("UTF-8"));
                    buffer.enterCriticalSection(Lock.WRITE);
                    try {
                        buffer.read(is, null, "TTL");   
                    } finally {
                        buffer.leaveCriticalSection() ;
                    }
                    logger.info("Buffer has "+buffer.size()+" triples...");
                } catch (UnsupportedEncodingException | RiotException ex) {
                    logger.warn(ex.toString());
                }
                if (buffer.size() > buffersize) {
                    flushRDF2QuadStore(buffer);
                    if (buffer.size() > maxbuffersize)  {
                        logger.warn("Too many triples!  Dumping to disk!");
                        DumpRDF2disk();
                    }
                }
            }
        } catch (IOException ex) {
            logger.warn(ex.toString());
        }
        logger.info("Ending PipeLogger...");
    }
    
    public void DumpRDF2disk() {
        OutputStream out;
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        Date date = new Date();
        String dump = logpath+"buffered-"+dateFormat.format(date)+".ttl";
        File f = new File(dump);
        Model Dump = ModelFactory.createDefaultModel();
        if (f.exists()) {
            Dump = RDFDataMgr.loadModel(dump);
            f.delete();
            buffer.enterCriticalSection(Lock.WRITE);
            try {
                Dump.add(buffer);
                buffer.removeAll();
            } finally {
                buffer.leaveCriticalSection();
            }
        }
        try {
            out = new FileOutputStream(dump);
            Dump.write(out,"TTL");
            Dump.removeAll();
        } catch (FileNotFoundException ex) {
            logger.error(ex.toString());
        }
    }
    
    public void LoadBufferedRDF() {
        File dir = new File(logpath+".");
        File [] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith("buffered-")&&name.endsWith(".ttl");
            }
        });
        if (files.length>0) {
            File ff = files[0];
            System.out.println(ff);
            Model k = RDFDataMgr.loadModel(ff.getName());
            if (flushRDF2QuadStore(k)) {
                ff.delete();
            }
        }
    }
    
    public boolean flushRDF2QuadStore(Model z) {
        boolean success = false;
        StringWriter writer = new StringWriter();
        z.enterCriticalSection(Lock.WRITE);
        try {
            z.write(writer,"TTL");
            try {    
                ContentResponse response = httpClient.newRequest(uri).method(HttpMethod.POST).content(new BytesContentProvider(writer.toString().getBytes()),"text/turtle").send();
                if (response.getStatus()==200) {
                    z.removeAll();
                    success = true;
                    flush = false;
                }               
            } finally {
                z.leaveCriticalSection();
            }
        } catch (InterruptedException | TimeoutException | ExecutionException ex) {
            logger.error(ex.toString());
        }
        return success;
    }
    
    class flushtask extends TimerTask {
        @Override
        public void run() {
            flush = true;
            LoadBufferedRDF();
            flushRDF2QuadStore(buffer);
    }
  }
}