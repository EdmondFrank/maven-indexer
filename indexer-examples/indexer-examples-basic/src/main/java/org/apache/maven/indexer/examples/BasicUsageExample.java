package org.apache.maven.indexer.examples;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.FutureRequestExecutionMetrics;
import org.apache.http.impl.client.HttpClients;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;
import org.apache.maven.index.ArtifactInfo;
import org.apache.maven.index.ArtifactInfoFilter;
import org.apache.maven.index.ArtifactInfoGroup;
import org.apache.maven.index.Field;
import org.apache.maven.index.FlatSearchRequest;
import org.apache.maven.index.FlatSearchResponse;
import org.apache.maven.index.GroupedSearchRequest;
import org.apache.maven.index.GroupedSearchResponse;
import org.apache.maven.index.Grouping;
import org.apache.maven.index.Indexer;
import org.apache.maven.index.IteratorSearchRequest;
import org.apache.maven.index.IteratorSearchResponse;
import org.apache.maven.index.MAVEN;
import org.apache.maven.index.context.IndexCreator;
import org.apache.maven.index.context.IndexUtils;
import org.apache.maven.index.context.IndexingContext;
import org.apache.maven.index.expr.SourcedSearchExpression;
import org.apache.maven.index.expr.UserInputSearchExpression;
import org.apache.maven.index.search.grouping.GAGrouping;
import org.apache.maven.index.updater.IndexUpdateRequest;
import org.apache.maven.index.updater.IndexUpdateResult;
import org.apache.maven.index.updater.IndexUpdater;
import org.apache.maven.index.updater.ResourceFetcher;
import org.apache.maven.index.updater.WagonHelper;
import org.apache.maven.wagon.Wagon;
import org.apache.maven.wagon.events.TransferEvent;
import org.apache.maven.wagon.events.TransferListener;
import org.apache.maven.wagon.observers.AbstractTransferListener;
import org.codehaus.plexus.DefaultContainerConfiguration;
import org.codehaus.plexus.DefaultPlexusContainer;
import org.codehaus.plexus.PlexusConstants;
import org.codehaus.plexus.PlexusContainer;
import org.codehaus.plexus.PlexusContainerException;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.codehaus.plexus.util.StringUtils;
import org.eclipse.aether.util.version.GenericVersionScheme;
import org.eclipse.aether.version.InvalidVersionSpecificationException;
import org.eclipse.aether.version.Version;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.io.IOException;

import java.io.InputStream;
import java.util.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Collection of some use cases.
 */
public class BasicUsageExample
{
    public static void main( String[] args ) throws Exception {
        final BasicUsageExample basicUsageExample = new BasicUsageExample();
        CommandLine line = parseArguments(args);
        if (line.hasOption("cache-dir") && line.hasOption("index-dir")) {
            String cacheDir = line.getOptionValue("cache-dir");
            String indexDir = line.getOptionValue("index-dir");
            String indexUrl = "https://repo1.maven.org/maven2";
            String nexusUrl = "http://mirrors.gitee.com/repository/maven-public";
            if (line.hasOption("index-url")) {
                indexUrl = line.getOptionValue("index-url");
            }
            if (line.hasOption("nexus-url")) {
                nexusUrl = line.getOptionValue("nexus-url");
            }
            boolean onlyStat = line.hasOption("stat");
            HashMap<String, String> options = new HashMap<String, String>();
            options.put("cache-dir", cacheDir);
            options.put("index-dir", indexDir);
            options.put("index-url", indexUrl);
            options.put("nexus-url", nexusUrl);
            options.put("only-stat", line.hasOption("stat") ? "1" : "0");
            basicUsageExample.perform(options);
        } else {
            printHelp();
        }
    }

    // ==

    private final PlexusContainer plexusContainer;

    private final Indexer indexer;

    private final IndexUpdater indexUpdater;

    private final Wagon httpWagon;

    private IndexingContext centralContext;

    public BasicUsageExample()
        throws PlexusContainerException, ComponentLookupException
    {
        // here we create Plexus container, the Maven default IoC container
        // Plexus falls outside of MI scope, just accept the fact that
        // MI is a Plexus component ;)
        // If needed more info, ask on Maven Users list or Plexus Users list
        // google is your friend!
        final DefaultContainerConfiguration config = new DefaultContainerConfiguration();
        config.setClassPathScanning( PlexusConstants.SCANNING_INDEX );
        this.plexusContainer = new DefaultPlexusContainer( config );

        // lookup the indexer components from plexus
        this.indexer = plexusContainer.lookup( Indexer.class );
        this.indexUpdater = plexusContainer.lookup( IndexUpdater.class );
        // lookup wagon used to remotely fetch index
        this.httpWagon = plexusContainer.lookup( Wagon.class, "http" );

    }

    public void perform(HashMap<String, String> options)
            throws IOException, ComponentLookupException, InvalidVersionSpecificationException, InterruptedException {
        // Files where local cache is (if any) and Lucene Index should be located
        // String cacheDir, String indexDir, String indexUrl, String nexusUrl
        File centralLocalCache = new File(options.get("cache-dir"));
        File centralIndexDir = new File(options.get("index-dir"));

        // Creators we want to use (search for fields it defines)
        List<IndexCreator> indexers = new ArrayList<>();
        indexers.add(plexusContainer.lookup( IndexCreator.class, "min" ));
        indexers.add(plexusContainer.lookup( IndexCreator.class, "jarContent" ));
        indexers.add(plexusContainer.lookup( IndexCreator.class, "maven-plugin" ));

        // Create context for central repository index
        centralContext = indexer.createIndexingContext(
                "central-context", "central",
                centralLocalCache, centralIndexDir,
                options.get("index-url"), null, true, true, indexers );

        // Update the index (incremental update will happen if this is not 1st run and files are not deleted)
        // This whole block below should not be executed on every app start, but rather controlled by some configuration
        // since this block will always emit at least one HTTP GET. Central indexes are updated once a week, but
        // other index sources might have different index publishing frequency.
        // Preferred frequency is once a week.
        if ( false )
        {
            System.out.println( "Updating Index..." );
            System.out.println( "This might take a while on first run, so please be patient!" );
            // Create ResourceFetcher implementation to be used with IndexUpdateRequest
            // Here, we use Wagon based one as shorthand, but all we need is a ResourceFetcher implementation
            TransferListener listener = new AbstractTransferListener()
            {
                public void transferStarted( TransferEvent transferEvent )
                {
                    System.out.print( "  Downloading " + transferEvent.getResource().getName() );
                }

                public void transferProgress( TransferEvent transferEvent, byte[] buffer, int length )
                {
                }

                public void transferCompleted( TransferEvent transferEvent )
                {
                    System.out.println( " - Done" );
                }
            };
            ResourceFetcher resourceFetcher = new WagonHelper.WagonFetcher( httpWagon, listener, null, null );

            Date centralContextCurrentTimestamp = centralContext.getTimestamp();
            IndexUpdateRequest updateRequest = new IndexUpdateRequest( centralContext, resourceFetcher );
            IndexUpdateResult updateResult = indexUpdater.fetchAndUpdateIndex( updateRequest );
            if ( updateResult.isFullUpdate() )
            {
                System.out.println( "Full update happened!" );
            }
            else if ( updateResult.getTimestamp().equals( centralContextCurrentTimestamp ) )
            {
                System.out.println( "No update needed, index is up to date!" );
            }
            else
            {
                System.out.println(
                    "Incremental update happened, change covered " + centralContextCurrentTimestamp + " - "
                        + updateResult.getTimestamp() + " period." );
            }

            System.out.println();
        }

        System.out.println();
        System.out.println( "Using index" );
        System.out.println( "===========" );
        System.out.println();

        // ====


        // Case:
        // dump all the GAVs
        // NOTE: will not actually execute do this below, is too long to do (Central is HUGE), but is here as code
        // example
        if (options.get("only-stat") != "1")
        {
            final IndexSearcher searcher = centralContext.acquireIndexSearcher();
            final ExecutorService executorService = Executors.newFixedThreadPool(16);
            try
            {
                final IndexReader ir = searcher.getIndexReader();
                Bits liveDocs = MultiFields.getLiveDocs( ir );
                int batchSize = 2000;
                List<Future> futures = new ArrayList<Future>();
                for ( int i = 0; i < ir.maxDoc(); i++ )
                {
                    if ( liveDocs == null || liveDocs.get( i ) )
                    {
                        final Document doc = ir.document( i );
                        final ArtifactInfo ai = IndexUtils.constructArtifactInfo( doc, centralContext );
                        futures.add(executorService.submit(() -> {
                            try {
                                String artifactUrl = options.get("nexus-url")
                                        + '/' + ai.getGroupId().replace('.','/')
                                        + '/' + ai.getArtifactId()
                                        + '/' + ai.getVersion()
                                        + '/' + ai.getArtifactId()
                                        + '-' + ai.getVersion()
                                        + '.' + ai.getFileExtension();
                                downloadFile(artifactUrl);
                            } catch (Exception e) {
                                System.out.println("failed to download:" + ai.getPath() + "\n" + e.toString());
                            }
                        }));
                        if( batchSize-- < 0) {
                           for(Future f: futures) {
                               try {
                                   f.get();
                               } catch (Exception e) {
                                   System.out.println("failed get result from future: " + e.toString());
                               }
                           }
                           futures.clear();
                           batchSize = 2000;
                        }

//                        System.out.println(ai.getGroupId() + ":" + ai.getArtifactId() + ":" + ai.getVersion() + ":"
//                                                + ai.getClassifier() + " (sha1=" + ai.getSha1() + ")" );
                    }

                }
                if (!futures.isEmpty()) {
                    for(Future f: futures) {
                        try {
                            f.get();
                        } catch (Exception e) {
                            System.out.println("failed get result from future: " + e.toString());
                        }
                    }
                    futures.clear();
                }
            } finally {
                centralContext.releaseIndexSearcher(searcher);
                executorService.shutdown();
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
            }
        } else {
            final IndexSearcher searcher = centralContext.acquireIndexSearcher();
            long totalSize = 0;
            try {
                final IndexReader ir = searcher.getIndexReader();
                System.out.println(ir.maxDoc());
                Bits liveDocs = MultiFields.getLiveDocs( ir );
                for ( int i = 0; i < ir.maxDoc(); i++ ) {
                    if ( liveDocs == null || liveDocs.get(i) ) {
                        final Document doc = ir.document(i);
                        final ArtifactInfo ai = IndexUtils.constructArtifactInfo(doc, centralContext);
                        if(ai != null) {
                            totalSize += ai.getSize();
                        }
                    }
                }
                System.out.println(sizeFmt(totalSize));
            } finally {
                centralContext.releaseIndexSearcher(searcher);
            }
        }

        // ====
        // Case:
        // Search for all GAVs with known G and A and having version greater than V
//
//        final GenericVersionScheme versionScheme = new GenericVersionScheme();
//        final String versionString = "1.5.0";
//        final Version version = versionScheme.parseVersion( versionString );
//
//        // construct the query for known GA
//        final Query groupIdQ =
//            indexer.constructQuery( MAVEN.GROUP_ID, new SourcedSearchExpression( "org.sonatype.nexus" ) );
//        final Query artifactIdQ =
//            indexer.constructQuery( MAVEN.ARTIFACT_ID, new SourcedSearchExpression( "nexus-api" ) );
//
//        final BooleanQuery query = new BooleanQuery.Builder()
//            .add( groupIdQ, Occur.MUST )
//            .add( artifactIdQ, Occur.MUST )
//            // we want "jar" artifacts only
//            .add( indexer.constructQuery( MAVEN.PACKAGING, new SourcedSearchExpression( "jar" ) ), Occur.MUST )
//            // we want main artifacts only (no classifier)
//            // Note: this below is unfinished API, needs fixing
//            .add( indexer.constructQuery( MAVEN.CLASSIFIER,
//                    new SourcedSearchExpression( Field.NOT_PRESENT ) ), Occur.MUST_NOT )
//            .build();
//
//        // construct the filter to express "V greater than"
//        final ArtifactInfoFilter versionFilter = new ArtifactInfoFilter()
//        {
//            public boolean accepts( final IndexingContext ctx, final ArtifactInfo ai )
//            {
//                try
//                {
//                    final Version aiV = versionScheme.parseVersion( ai.getVersion() );
//                    // Use ">=" if you are INCLUSIVE
//                    return aiV.compareTo( version ) > 0;
//                }
//                catch ( InvalidVersionSpecificationException e )
//                {
//                    // do something here? be safe and include?
//                    return true;
//                }
//            }
//        };
//
//        System.out.println(
//            "Searching for all GAVs with G=org.sonatype.nexus and nexus-api and having V greater than 1.5.0" );
//        final IteratorSearchRequest request =
//            new IteratorSearchRequest( query, Collections.singletonList( centralContext ), versionFilter );
//        final IteratorSearchResponse response = indexer.searchIterator( request );
//        for ( ArtifactInfo ai : response )
//        {
//            System.out.println( ai.toString() );
//        }
//
//        // Case:
//        // Use index
//        // Searching for some artifact
//        Query gidQ =
//            indexer.constructQuery( MAVEN.GROUP_ID, new SourcedSearchExpression( "org.apache.maven.indexer" ) );
//        Query aidQ = indexer.constructQuery( MAVEN.ARTIFACT_ID, new SourcedSearchExpression( "indexer-artifact" ) );
//
//        BooleanQuery bq = new BooleanQuery.Builder()
//                .add( gidQ, Occur.MUST )
//                .add( aidQ, Occur.MUST )
//                .build();
//
//        searchAndDump( indexer, "all artifacts under GA org.apache.maven.indexer:indexer-artifact", bq );
//
//        // Searching for some main artifact
//        bq = new BooleanQuery.Builder()
//                .add( gidQ, Occur.MUST )
//                .add( aidQ, Occur.MUST )
////                .add( indexer.constructQuery( MAVEN.CLASSIFIER, new SourcedSearchExpression( "*" ) ), Occur.MUST_NOT )
//                .build();
//
//        searchAndDump( indexer, "main artifacts under GA org.apache.maven.indexer:indexer-artifact", bq );
//
//        // doing sha1 search
//        searchAndDump( indexer, "SHA1 7ab67e6b20e5332a7fb4fdf2f019aec4275846c2",
//                       indexer.constructQuery( MAVEN.SHA1,
//                                               new SourcedSearchExpression( "7ab67e6b20e5332a7fb4fdf2f019aec4275846c2" )
//                       )
//        );
//
//        searchAndDump( indexer, "SHA1 7ab67e6b20 (partial hash)",
//                       indexer.constructQuery( MAVEN.SHA1, new UserInputSearchExpression( "7ab67e6b20" ) ) );
//
//        // doing classname search (incomplete classname)
//        searchAndDump( indexer, "classname DefaultNexusIndexer (note: Central does not publish classes in the index)",
//                       indexer.constructQuery( MAVEN.CLASSNAMES,
//                                               new UserInputSearchExpression( "DefaultNexusIndexer" ) ) );
//
//        // doing search for all "canonical" maven plugins latest versions
//        bq = new BooleanQuery.Builder()
//            .add( indexer.constructQuery( MAVEN.PACKAGING, new SourcedSearchExpression( "maven-plugin" ) ), Occur.MUST )
//            .add( indexer.constructQuery( MAVEN.GROUP_ID,
//                    new SourcedSearchExpression( "org.apache.maven.plugins" ) ), Occur.MUST )
//            .build();
//
//        searchGroupedAndDump( indexer, "all \"canonical\" maven plugins", bq, new GAGrouping() );
//
//        // doing search for all archetypes latest versions
//        searchGroupedAndDump( indexer, "all maven archetypes (latest versions)",
//                              indexer.constructQuery( MAVEN.PACKAGING,
//                                                      new SourcedSearchExpression( "maven-archetype" ) ),
//                              new GAGrouping() );

        // close cleanly
        indexer.closeIndexingContext( centralContext, false );
    }

    public void searchAndDump( Indexer nexusIndexer, String descr, Query q )
        throws IOException
    {
        System.out.println( "Searching for " + descr );

        FlatSearchResponse response = nexusIndexer.searchFlat( new FlatSearchRequest( q, centralContext ) );

        for ( ArtifactInfo ai : response.getResults() )
        {
            System.out.println( ai.toString() );
        }

        System.out.println( "------" );
        System.out.println( "Total: " + response.getTotalHitsCount() );
        System.out.println();
    }

    private static final int MAX_WIDTH = 60;

    public void searchGroupedAndDump( Indexer nexusIndexer, String descr, Query q, Grouping g )
        throws IOException
    {
        System.out.println( "Searching for " + descr );

        GroupedSearchResponse response = nexusIndexer.searchGrouped( new GroupedSearchRequest( q, g, centralContext ) );

        for ( Map.Entry<String, ArtifactInfoGroup> entry : response.getResults().entrySet() )
        {
            ArtifactInfo ai = entry.getValue().getArtifactInfos().iterator().next();
            System.out.println( "* Entry " + ai );
            System.out.println( "  Latest version:  " + ai.getVersion() );
            System.out.println( StringUtils.isBlank( ai.getDescription() )
                                    ? "No description in plugin's POM."
                                    : StringUtils.abbreviate( ai.getDescription(), MAX_WIDTH ) );
            System.out.println();
        }

        System.out.println( "------" );
        System.out.println( "Total record hits: " + response.getTotalHitsCount() );
        System.out.println();
    }

    private static Options getOptions() {
        Options options = new Options();
        options.addOption("c","cache-dir", true, "the path to local central cache dir");
        options.addOption("i", "index-dir", true, "the path to local lucene dir");
        options.addOption("r", "index-url", true, "the parent url of remote central index" );
        options.addOption("n","nexus-url", true, "the repository url of nexus");
        options.addOption("s", "stat", false, "only get artifacts statistics information");
        return options;
    }

    private static void downloadFile(String uri) throws Exception {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(uri);
        CloseableHttpResponse response = null;
        try {
            response = httpclient.execute(httpGet);
            if(response.getStatusLine().getStatusCode() == 200) {
                System.out.println("downloaded " + uri + " successfully!");
            } else {
                System.out.println("failed to downloaded " + uri);
            }
        } finally {
            if (response != null) {
                response.close();
            }
            httpclient.close();
        }
    }
    private static String sizeFmt(long num) {
        return sizeFmt(num, "iB");
    }
    private static String sizeFmt(long dnum, String suffix) {
        String[] units = {"", "K", "M", "G", "T", "P", "E", "Z"};
        double num = (double) dnum;
        for (String unit : units) {
            if (Math.abs(num) < 1024) {
                return String.format("%,.2f%s%s", num, unit, suffix);
            }
            num /= 1024.0;
        }
        return String.format("%,.2f%s%s", num, 'Y', suffix);
    }
    private static void printHelp() {
        Options options = getOptions();
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Run with java -jar:", options, true);
    }

    private static CommandLine parseArguments(String[] args) {
        Options options = getOptions();
        CommandLine line = null;
        CommandLineParser parser = new DefaultParser();
        try {
            line = parser.parse(options, args);
        } catch (ParseException ex) {
            System.err.println("Failed to parse command line arguments");
            System.err.println(ex.toString());
            printHelp();
            System.exit(1);
        }
        return line;
    }
}
