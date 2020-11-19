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

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.http.client.methods.HttpHead;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;
import org.apache.maven.index.ArtifactInfo;
import org.apache.maven.index.Indexer;
import org.apache.maven.index.MAVEN;
import org.apache.maven.index.context.IndexCreator;
import org.apache.maven.index.context.IndexUtils;
import org.apache.maven.index.context.IndexingContext;
import org.apache.maven.index.expr.SourcedSearchExpression;
import org.apache.maven.index.updater.IndexUpdateRequest;
import org.apache.maven.index.updater.IndexUpdateResult;
import org.apache.maven.index.updater.IndexUpdater;
import org.apache.maven.index.updater.ResourceFetcher;
import org.apache.maven.index.updater.WagonHelper;
import org.apache.maven.wagon.Wagon;
import org.apache.maven.wagon.events.TransferEvent;
import org.apache.maven.wagon.events.TransferListener;
import org.apache.maven.wagon.observers.AbstractTransferListener;
import org.apache.maven.wagon.proxy.ProxyInfo;
import org.codehaus.plexus.DefaultContainerConfiguration;
import org.codehaus.plexus.DefaultPlexusContainer;
import org.codehaus.plexus.PlexusConstants;
import org.codehaus.plexus.PlexusContainer;
import org.codehaus.plexus.PlexusContainerException;
import org.codehaus.plexus.util.StringUtils;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.io.IOException;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Collection of some use cases.
 */
public class BasicUsageExample
{
    private static String centralUrl = "";
    private static String nexusUrl = "";
    private static String globalProxy = "";
    private static int workers = 8;
    private static int producers = 4;

    public static void main( String[] args ) throws Exception {
        final BasicUsageExample basicUsageExample = new BasicUsageExample();
        CommandLine line = parseArguments(args);
        if (line.hasOption("cache-dir") && line.hasOption("index-dir")) {
            String cacheDir = line.getOptionValue("cache-dir");
            String indexDir = line.getOptionValue("index-dir");
            String indexUrl = "https://repo1.maven.org/maven2";
            String nexusUrl = "http://mirrors.gitee.com/repository/maven-public";
            String groupId = "";
            String artifactId = "";
            String lastIndex = "0";
            if (line.hasOption("index-url")) {
                indexUrl = line.getOptionValue("index-url");
            }
            if (line.hasOption("nexus-url")) {
                nexusUrl = line.getOptionValue("nexus-url");
            }
            if (line.hasOption("proxy")) {
                BasicUsageExample.globalProxy = line.getOptionValue("proxy");
            }
            if (line.hasOption("workers")) {
                try {
                    BasicUsageExample.workers  = Integer.parseInt(line.getOptionValue("workers"));
                } catch (NumberFormatException ex) {
                    BasicUsageExample.workers = 8;
                }
            }
            if (line.hasOption("producers")) {
                try {
                    BasicUsageExample.producers  = Integer.parseInt(line.getOptionValue("producers"));
                } catch (NumberFormatException ex) {
                    BasicUsageExample.producers = 4;
                }
            }
            if (line.hasOption("last-sync")) {
                String point = line.getOptionValue("last-sync");

                if (StringUtils.isNumeric(point)) {
                    lastIndex = point;
                } else {
                    String [] gav = line.getOptionValue("last-sync").split(",");
                    if (gav.length == 2) {
                        groupId = gav[0];
                        artifactId = gav[1];
                        System.out.println("begin point : " + groupId + " " + artifactId);
                    }
                }
            }
            HashMap<String, String> options = new HashMap<>();
            BasicUsageExample.centralUrl = indexUrl;
            BasicUsageExample.nexusUrl = nexusUrl;
            options.put("cache-dir", cacheDir);
            options.put("index-dir", indexDir);
            options.put("last-group-id", groupId);
            options.put("last-artifact-id", artifactId);
            options.put("last-index", lastIndex);
            options.put("only-stat", line.hasOption("stat") ? "1" : "0");
            options.put("update-index", line.hasOption("update") ? "1" : "0");

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
            throws IOException, ComponentLookupException, InterruptedException {
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
                BasicUsageExample.centralUrl, null, true, true, indexers );

        // Update the index (incremental update will happen if this is not 1st run and files are not deleted)
        // This whole block below should not be executed on every app start, but rather controlled by some configuration
        // since this block will always emit at least one HTTP GET. Central indexes are updated once a week, but
        // other index sources might have different index publishing frequency.
        // Preferred frequency is once a week.

        if (options.get("update-index") == "1") {
            System.out.println("Updating Index...");
            System.out.println("This might take a while on first run, so please be patient!");
            // Create ResourceFetcher implementation to be used with IndexUpdateRequest
            // Here, we use Wagon based one as shorthand, but all we need is a ResourceFetcher implementation
            TransferListener listener = new AbstractTransferListener() {
                private int count = 0;
                private int col = 0;
                private int kb = 0;

                public void transferStarted(TransferEvent transferEvent) {
                    System.out.println("  Downloading " + transferEvent.getResource().getName());
                }

                public void transferProgress(TransferEvent transferEvent, byte[] buffer, int length) {
                    final int unitChunk = 64;
                    long totalLength = transferEvent.getResource().getContentLength();
                    if (buffer == null) {
                        return;
                    }

                    count += buffer.length;

                    if ((count / unitChunk) > kb) {
                        if (col > 80) {
                            System.out.println(String.format("%s/%s", sizeFmt(count), sizeFmt(totalLength)));
                            col = 0;
                        }

                        System.out.print('.');
                        col++;
                        kb++;
                    }
                }

                public void transferCompleted(TransferEvent transferEvent) {
                    System.out.println(" - Done");
                }
            };

            ProxyInfo proxy = new ProxyInfo();
            if (BasicUsageExample.globalProxy != "") {
                try {
                    String[] ipInfo = BasicUsageExample.globalProxy.split(":");
                    proxy.setHost(ipInfo[0]);
                    proxy.setPort(Integer.parseInt(ipInfo[1], 10));
                    proxy.setType("HTTP");
                } catch (Exception e) {
                    System.out.println("invaild proxy! correct format: host:<port>");
                    throw e;
                }
            }
            ResourceFetcher resourceFetcher = new WagonHelper.WagonFetcher(httpWagon, listener, null, proxy);
            Date centralContextCurrentTimestamp = centralContext.getTimestamp();
            IndexUpdateRequest updateRequest = new IndexUpdateRequest(centralContext, resourceFetcher);
            IndexUpdateResult updateResult = indexUpdater.fetchAndUpdateIndex(updateRequest);

            if (updateResult.isFullUpdate()) {
                System.out.println("Full update happened!");
            } else if (updateResult.getTimestamp().equals(centralContextCurrentTimestamp)) {
                System.out.println("No update needed, index is up to date!");
            } else {
                System.out.println(
                        "Incremental update happened, change covered " + centralContextCurrentTimestamp + " - "
                                + updateResult.getTimestamp() + " period.");
            }
        }
        System.out.println( "Using index" );
        System.out.println( "===========" );

        // ====


        // Case:
        // dump all the GAVs
        // NOTE: will not actually execute do this below, is too long to do (Central is HUGE), but is here as code
        // example
        if (options.get("only-stat") != "1") {
            final IndexSearcher searcher = centralContext.acquireIndexSearcher();
            final ThreadPoolExecutor executorService = new ThreadPoolExecutor(
                    BasicUsageExample.producers,
                    BasicUsageExample.producers,
                    0L,
                    TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(BasicUsageExample.producers),
                    new ThreadPoolExecutor.CallerRunsPolicy());
            final ThreadPoolExecutor workerService = new ThreadPoolExecutor(
                    BasicUsageExample.workers,
                    BasicUsageExample.workers,
                    0L,
                    TimeUnit.MICROSECONDS,
                    new ArrayBlockingQueue<>(BasicUsageExample.workers),
                    new ThreadPoolExecutor.CallerRunsPolicy());
            try
            {
                boolean haslastPoint = false;
                ArtifactInfo lastPoint = null;
                boolean begin = false;
                String lastGroupId = options.get("last-group-id");
                String lastArtifactId = options.get("last-artifact-id");
                int lastIndex = Integer.parseInt(options.get("last-index"), 10);
                if (lastGroupId != "" && lastArtifactId != "") {
                    Query gidQ = indexer.constructQuery(MAVEN.GROUP_ID, new SourcedSearchExpression(lastGroupId));
                    Query aidQ = indexer.constructQuery( MAVEN.ARTIFACT_ID, new SourcedSearchExpression(lastArtifactId));
                    BooleanQuery bq = new BooleanQuery.Builder()
                        .add( gidQ, Occur.MUST )
                        .add( aidQ, Occur.MUST )
                        .build();
                    lastPoint = searchExistResult(indexer, bq);
                    if (lastPoint != null) {
                        haslastPoint = true;
                    }
                }
                final IndexReader ir = searcher.getIndexReader();
                Bits liveDocs = MultiFields.getLiveDocs( ir );
                ArtifactInfo prev = null;
                final int total = ir.maxDoc();
                for ( int i = lastIndex; i < ir.maxDoc(); i++)
                {
                    if ( liveDocs == null || liveDocs.get( i ) )
                    {

                        final Document doc = ir.document( i );
                        final ArtifactInfo ai = IndexUtils.constructArtifactInfo( doc, centralContext );
                        if (ai != null) {
                            if (haslastPoint && !begin) {
                                String currentGroupId = ai.getGroupId();
                                String currentArtifactId = ai.getArtifactId();
                                if (lastPoint.getGroupId().equals(currentGroupId) &&
                                        lastPoint.getArtifactId().equals(currentArtifactId)) {
                                    begin = true;
                                } else {
                                    System.out.println("skiped " + currentGroupId + " " + currentArtifactId);
                                    continue;
                                }
                            }

                            if (prev != null &&
                                    prev.getGroupId().equals(ai.getGroupId()) &&
                                    prev.getArtifactId().equals(ai.getArtifactId()) &&
                                    prev.getVersion().equals((ai.getVersion()))) {
                                continue;
                            }
                            String artifactPath = '/' + ai.getGroupId().replace('.','/')
                                    + '/' + ai.getArtifactId()
                                    + '/' + ai.getVersion();
                            final int index = i;
                            executorService.submit(() -> {
                                    try {
                                        fetchDir(index, total, artifactPath, workerService);
                                    } catch (Exception e) {
                                        System.out.println("failed to fetch:" + artifactPath + "\n" + e.toString());
                                    }
                                });
                            prev = ai;
                        }
                    }
                }
            } finally {
                centralContext.releaseIndexSearcher(searcher);
                executorService.shutdown();
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
                workerService.shutdown();
                workerService.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
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

        // close cleanly
        indexer.closeIndexingContext( centralContext, false );
    }

    public ArtifactInfo searchExistResult(Indexer nexusIndexer, Query q) throws IOException {
        Collection<ArtifactInfo> response = nexusIndexer.identify(q, Collections.singletonList(centralContext));
        return response.iterator().hasNext() ? response.iterator().next() : null;
    }

    private static Options getOptions() {
        Options options = new Options();
        options.addOption("c", "cache-dir", true, "the path to local central cache dir");
        options.addOption("i", "index-dir", true, "the path to local lucene dir");
        options.addOption("r", "index-url", true, "the parent url of remote central index" );
        options.addOption("n", "nexus-url", true, "the repository url of nexus");
        options.addOption("l", "last-sync", true, "set last sync point");
        options.addOption("x", "proxy", true, "set proxy for index updater");
        options.addOption("w", "workers", true,"set nums of download workers");
        options.addOption("p", "producers", true, "set nums of url fetchers");
        options.addOption("s", "stat", false, "only get artifacts statistics information");
        options.addOption("U", "update", false, "update index if upstream allow");
        return options;
    }

    private static void fetchDir (int index, int total, String subDir, ThreadPoolExecutor workerService) {
        String content = httpGetText(buildCentralUrl(subDir));
        List<String> linkList = new ArrayList<>();
        final String linkRegex = "<a href=\"(.*?)\".*>(.*?)<\\/a>";
        final Pattern linkPattern = Pattern.compile(linkRegex);
        Matcher matcher = linkPattern.matcher(content);
        while (matcher.find()) {
            String href = matcher.group(1);
            linkList.add(href);
        }
        for (String artifactUrl : linkList) {
            if (artifactUrl != null && !artifactUrl.startsWith("..") && !artifactUrl.endsWith("/")) {
                String pkgUrl = buildNexusUrl(subDir + "/" + artifactUrl);
                workerService.submit(() -> {
                    try {
                        if(!existFile(pkgUrl)) {
                            downloadFile(pkgUrl);
                            System.out.println(String.format("[%d/%d] successfully downloaded: %s", index, total, pkgUrl));
                        } else {
                            System.out.println(String.format("[%d/%d] existed: %s", index, total, pkgUrl));
                        }
                    } catch (Exception e) {
                        System.out.println(String.format("[%d/%d] failed to download: %s error: %s", index, total, pkgUrl, e.toString()));
                    }
                });
            }
        }
    }

    private static boolean existFile(String uri) throws Exception {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpHead head = new HttpHead(uri);
        CloseableHttpResponse response = null;
        try {
            response = httpclient.execute(head);
            return response.getStatusLine().getStatusCode() == 200;
        } finally {
            if (response != null) {
                response.close();
            }
            httpclient.close();
        }
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
                int code = response.getStatusLine().getStatusCode();
                switch (code) {
                case 404:
                    System.out.println("[404] Not found failed to downloaded " + uri);
                    break;
                case 500:
                    System.out.println("[500] Server error failed to downloaded " + uri);
                    break;
                default:
                    System.out.println(String.format("[%d] Unknown error failed to downloaded ", code) + uri);
                }
            }
        } finally {
            if (response != null) {
                response.close();
            }
            httpclient.close();
        }
    }

    public static String httpGetText(String url) {
        String result = "";
        HttpGet request = buildRequest(url, 15);
        try (CloseableHttpClient httpClient = HttpClients.createDefault();
             CloseableHttpResponse response = httpClient.execute(request)) {
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                result = EntityUtils.toString(entity);
            }
        } finally {
            return result;
        }
    }

    private static HttpGet buildRequest(String url, int timeoutInSeconds) {
        RequestConfig requestConfig;
        Builder builder = RequestConfig.custom()
                .setConnectionRequestTimeout(timeoutInSeconds * 1000)
                .setSocketTimeout(timeoutInSeconds * 1000);
        requestConfig = builder.build();
        HttpGet request = new HttpGet(url);
        request.setConfig(requestConfig);
        request.setHeader("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36");
        return request;
    }

    private static String buildCentralUrl(String relativePath) {
        return String.format("%s%s", BasicUsageExample.centralUrl, relativePath);
    }

    private static String buildNexusUrl(String relativePath) {
        return String.format("%s%s", BasicUsageExample.nexusUrl, relativePath);
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
