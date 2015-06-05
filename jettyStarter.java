/** 
 * MongoWorkflow.java 
 * 
 * Copyright 2013 President and Fellows of Harvard College
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author Tianhong Song
 * @author bluecobalt27
 * @author chicoreus
 */

package org.filteredpush.akka.workflows;

import akka.actor.*;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.filteredpush.akka.actors.BasisOfRecordValidator;
import org.filteredpush.akka.actors.GEORefValidator;
import org.filteredpush.akka.actors.InternalDateValidator;
import org.filteredpush.akka.actors.NewScientificNameValidator;
import org.filteredpush.akka.actors.io.IDigBioReader;
import org.filteredpush.akka.actors.io.MongoSummaryWriter;
import org.filteredpush.akka.data.Curate;
import org.filteredpush.akka.data.Prov;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/*
* @begin MongoWorkflow
* @param inputFilename
* @param outputFilename
* @param nameAuthority
* @in inputFile @uri {mongoHost}{db}{inputCollection}
* @out outputFile @uri {mongoHost}{db}{outputCollection} 
*/
public class jettyStarter  {

    public static void main(String[] args) {
        final jettyStarter fp = new jettyStarter();
        /* @begin ParseOptions
         * @call setup
         * @param inputFilename
         * @param outputFilename
         * @param nameAuthority
         * @out serviceClass @as nameService
         */

        fp.setup(args);

        Server server = new Server(Integer.valueOf(port));

        System.out.println("Jetty server started at " + Integer.valueOf(port) +" ...");

        server.setHandler(new AbstractHandler() {

            public void handle(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response) throws IOException, ServletException {
                response.setContentType("text/html;charset=utf-8");
                response.setStatus(HttpServletResponse.SC_OK);
                baseRequest.setHandled(true);

                //baseRequest.extractParameters();

                String limit = baseRequest.getParameter("limit");
                String rq = baseRequest.getParameter("rq");
                String authority = baseRequest.getParameter("authority");
                String workflow = baseRequest.getParameter("workflow");
                String tax = baseRequest.getParameter("tax");
                boolean taxMode = false;
                if(tax.equals("true")) taxMode = true;

                response.getWriter().println("Running analysis...");

                fp.calculate(limit, rq, workflow, authority, taxMode);

            }

        });


        try {
            server.start();
            server.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Option(name="-o",usage="output JSON file")
    //private String outputFilename = "/home/thsong/data/scan_data/test.json";
    private String outputFilename = "output.json";

    @Option(name="-p",usage="port to use")
    private static String port = "8088";
    //private String inputFilename = "input.txt";


    public void setup(String[] args) {
        CmdLineParser parser = new CmdLineParser(this);
        parser.setUsageWidth(4096);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            e.printStackTrace();
        }
    }

    public void calculate(final String limit, final String rq, final String workflowToUse, final String authority, final boolean taxMode) {

        long starttime = System.currentTimeMillis();

        // Create an Akka system
        final  ActorSystem system = ActorSystem.create("FpSystem");

        final ActorRef writer = system.actorOf(new Props(new UntypedActorFactory() {
            public UntypedActor create() {
                return new MongoSummaryWriter(outputFilename);
            }
        }), "MongoDBWriter");
        /* @end MongoSummaryWriter */

        /* @begin GEORefValidator
         * @in dateValidatedRecords
         * @out geoRefValidatedRecords
         */
        final ActorRef geoValidator = system.actorOf(new Props(new UntypedActorFactory() {
            public UntypedActor create() {
                return new GEORefValidator("org.filteredpush.kuration.services.GeoLocate3",false,200.0, writer);
            }
        }), "geoValidator");
        /* @end GEORefValidator */

        /* @begin InternalDateValidator
         * @in borValidatedRecords
         * @out dateValidatedRecords
         */
        final ActorRef dateValidator = system.actorOf(new Props(new UntypedActorFactory() {
            public UntypedActor create() {
                return new InternalDateValidator("org.filteredpush.kuration.services.InternalDateValidationService", geoValidator);
            }
        }), "dateValidator");
        /* @end InternalDateValidator */

        /* @begin BasisOfRecordValidator
         * @in nameValidatedRecords
         * @out borValidatedRecords
         */
        final ActorRef basisOfRecordValidator = system.actorOf(new Props(new UntypedActorFactory() {
            public UntypedActor create() {
                return new BasisOfRecordValidator("org.filteredpush.kuration.services.BasisOfRecordValidationService", geoValidator);
            }
        }), "basisOfRecordValidator");
        /* @end BasisOfRecordValidator */

        /* @begin ScientificNameValidator
         * @param service @as nameService
         * @in inputSpecimenRecords
         * @out nameValidatedRecords
         */
        final ActorRef scinValidator = system.actorOf(new Props(new UntypedActorFactory() {
            public UntypedActor create() {
                // TODO: Need to see if this sort of picking inside create() will work
                // to allow choice between CSV or MongoDB input from command line parameters
                // letting DwCaWorkflow and MongoWorkflow be collapsed into a single workflow.
                if (authority.toUpperCase().equals("GLOBALNAMES")) {
                    return new SciNameWorkflow("-t",false,basisOfRecordValidator);
                } else {
                    boolean useCache = true;
                    boolean insertGuid = true;
                    return new NewScientificNameValidator(useCache,insertGuid,authority, taxMode, basisOfRecordValidator);
                }
            }
        }), "scinValidator");


        final ActorRef reader = system.actorOf(new Props(new UntypedActorFactory() {
            public UntypedActor create() {
                return new IDigBioReader(limit, rq, scinValidator);
            }
        }), "reader");
        /* @end CSVReader */


        // start the calculation
        System.err.println("systemstart#"+" " + "#" + System.currentTimeMillis());
        reader.tell(new Curate());
        //system.shutdown();
        system.awaitTermination();
        long stoptime = System.currentTimeMillis();
        //System.out.printf("\nTime: %f s\n",(stoptime-starttime)/1000.0);
        System.err.printf("runtime: %d\n",stoptime-starttime);

    }


}
/* @end MongoWorkflow */
