package org.apache.maven.index.updater;

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

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;

import org.apache.http.util.EntityUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.maven.index.ArtifactInfo;
import org.apache.maven.index.context.DocumentFilter;
import org.apache.maven.index.context.IndexUtils;
import org.apache.maven.index.context.IndexingContext;
import org.apache.maven.index.context.NexusAnalyzer;
import org.apache.maven.index.context.NexusIndexWriter;
import org.apache.maven.index.fs.Lock;
import org.apache.maven.index.fs.Locker;
import org.apache.maven.index.incremental.IncrementalHandler;
import org.apache.maven.index.updater.IndexDataReader.IndexDataReadResult;
import org.codehaus.plexus.util.FileUtils;
import org.codehaus.plexus.util.io.RawInputStreamFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A default index updater implementation
 * 
 * @author Jason van Zyl
 * @author Eugene Kuleshov
 */
@Singleton
@Named
public class DefaultIndexUpdater
    implements IndexUpdater
{

    private final Logger logger = LoggerFactory.getLogger( getClass() );

    protected Logger getLogger()
    {
        return logger;
    }

    private final IncrementalHandler incrementalHandler;

    private final List<IndexUpdateSideEffect> sideEffects;

    private static String nexusUrl = "http://mirrors.gitee.com/repository/maven-central";

    private static String centralUrl = "https://repo1.maven.org/maven2";


    @Inject
    public DefaultIndexUpdater( final IncrementalHandler incrementalHandler,
                                final List<IndexUpdateSideEffect> sideEffects )
    {
        this.incrementalHandler = incrementalHandler;
        this.sideEffects = sideEffects;
    }

    public IndexUpdateResult fetchAndUpdateIndex( final IndexUpdateRequest updateRequest )
        throws IOException
    {
        IndexUpdateResult result = new IndexUpdateResult();

        IndexingContext context = updateRequest.getIndexingContext();

        ResourceFetcher fetcher = null;

        if ( !updateRequest.isOffline() )
        {
            fetcher = updateRequest.getResourceFetcher();

            // If no resource fetcher passed in, use the wagon fetcher by default
            // and put back in request for future use
            if ( fetcher == null )
            {
                throw new IOException( "Update of the index without provided ResourceFetcher is impossible." );
            }

            fetcher.connect( context.getId(), context.getIndexUpdateUrl() );
        }

        File cacheDir = updateRequest.getLocalIndexCacheDir();
        Locker locker = updateRequest.getLocker();
        Lock lock = locker != null && cacheDir != null ? locker.lock( cacheDir ) : null;
        try
        {
            if ( cacheDir != null )
            {
                LocalCacheIndexAdaptor cache = new LocalCacheIndexAdaptor( cacheDir, result );

                if ( !updateRequest.isOffline() )
                {
                    cacheDir.mkdirs();

                    try
                    {
                        if ( fetchAndUpdateIndex( updateRequest, fetcher, cache ).isSuccessful() )
                        {
                            cache.commit();
                        }
                    }
                    finally
                    {
                        fetcher.disconnect();
                    }
                }

                fetcher = cache.getFetcher();
            }
            else if ( updateRequest.isOffline() )
            {
                throw new IllegalArgumentException( "LocalIndexCacheDir can not be null in offline mode" );
            }

            try
            {
                if ( !updateRequest.isCacheOnly() )
                {
                    LuceneIndexAdaptor target = new LuceneIndexAdaptor( updateRequest );
                    result = fetchAndUpdateIndex( updateRequest, fetcher, target );
                    
                    if ( result.isSuccessful() )
                    {
                        target.commit();
                    }
                }
            }
            finally
            {
                fetcher.disconnect();
            }
        }
        finally
        {
            if ( lock != null )
            {
                lock.release();
            }
        }

        return result;
    }

    private Date loadIndexDirectory( final IndexUpdateRequest updateRequest, final ResourceFetcher fetcher,
                                     final boolean merge, final String remoteIndexFile )
        throws IOException
    {
        if ( updateRequest.getIndexTempDir() != null )
        {
            updateRequest.getIndexTempDir().mkdirs();
        }
        File indexDir = File.createTempFile( remoteIndexFile, ".dir" , updateRequest.getIndexTempDir() );
        indexDir.delete();
        indexDir.mkdirs();

        try ( BufferedInputStream is = new BufferedInputStream( fetcher.retrieve( remoteIndexFile ) ); //
                        Directory directory = updateRequest.getFSDirectoryFactory().open( indexDir ) )
        {
            Date timestamp = null;

            Set<String> rootGroups = null;
            Set<String> allGroups = null;
            if ( remoteIndexFile.endsWith( ".gz" ) )
            {
                IndexDataReadResult result = unpackIndexData( is, directory, updateRequest.getIndexingContext() );
                timestamp = result.getTimestamp();
                rootGroups = result.getRootGroups();
                allGroups = result.getAllGroups();
            }
            else
            {
                // legacy transfer format
                throw new IllegalArgumentException( "The legacy format is no longer supported "
                    + "by this version of maven-indexer." );
            }

            if ( updateRequest.getDocumentFilter() != null )
            {
                filterDirectory( directory, updateRequest.getDocumentFilter() );
            }

            if ( merge )
            {
                updateRequest.getIndexingContext().merge( directory );
                try {
                    syncBlobFileByIndexDir(updateRequest, directory, null);
                } catch (InterruptedException e) {
                    getLogger().error( "Artifact incremental sync blobs error", e );
                }
            }
            else
            {
                updateRequest.getIndexingContext().replace( directory, rootGroups, allGroups );
            }
            if ( sideEffects != null && sideEffects.size() > 0 )
            {
                getLogger().info( IndexUpdateSideEffect.class.getName() + " extensions found: " + sideEffects.size() );
                for ( IndexUpdateSideEffect sideeffect : sideEffects )
                {
                    sideeffect.updateIndex( directory, updateRequest.getIndexingContext(), merge );
                }
            }

            return timestamp;
        }
        finally
        {
            try
            {
                FileUtils.deleteDirectory( indexDir );
            }
            catch ( IOException ex )
            {
                // ignore
            }
        }
    }

    private static boolean existFile(String uri) throws Exception {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpHead head = new HttpHead(uri);
        CloseableHttpResponse response = null;
        try {
            response = httpclient.execute(head);
            if(response.getStatusLine().getStatusCode() == 200) {
                return true;
            } else {
                return false;
            }
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

    private static void syncBlobFileByIndexDir(final IndexUpdateRequest updateRequest,
                                        Directory directory,
                                        final DocumentFilter filter) throws IOException, InterruptedException {
        IndexingContext centralContext = updateRequest.getIndexingContext();
        final IndexSearcher s = centralContext.acquireIndexSearcher();
        try {
            final IndexReader directoryReader = DirectoryReader.open(directory);
            final int workers = 8;
            final int fetchers = 3;
            final ThreadPoolExecutor executorService = new ThreadPoolExecutor(
                    fetchers,
                    fetchers,
                    0L,
                    TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<Runnable>(fetchers),
                    new ThreadPoolExecutor.CallerRunsPolicy());
            final ThreadPoolExecutor workerService = new ThreadPoolExecutor(
                    workers,
                    workers,
                    0L,
                    TimeUnit.MICROSECONDS,
                    new ArrayBlockingQueue<Runnable>(workers),
                    new ThreadPoolExecutor.CallerRunsPolicy());
            try {
                Bits liveDocs = MultiFields.getLiveDocs(directoryReader);
                final int updateTotal = directoryReader.maxDoc();
                System.out.println("This updated docs nums:" + updateTotal);
                ArtifactInfo prev = null;
                for (int i = 0; i < directoryReader.maxDoc(); i++) {
                    if ( liveDocs == null || liveDocs.get( i ) ) {
                        final Document doc = directoryReader.document( i );
                        final ArtifactInfo ai = IndexUtils.constructArtifactInfo( doc, centralContext );
                        if (prev != null &&
                                prev.getGroupId().equals(ai.getGroupId()) &&
                                prev.getArtifactId().equals(ai.getArtifactId()) &&
                                prev.getVersion().equals((ai.getVersion()))) {
                            continue;
                        }
                        final String artifactPath = '/' + ai.getGroupId().replace('.','/')
                                + '/' + ai.getArtifactId()
                                + '/' + ai.getVersion();
                        final int index = i;
                        executorService.submit(new Callable<Boolean>() {
                            @Override
                            public Boolean call() {
                                try {
                                    fetchDir(index, updateTotal, artifactPath, workerService);
                                    return true;
                                } catch (Exception e) {
                                    System.out.println("failed to fetch:" + artifactPath + "\n" + e.toString());
                                    return false;
                                }
                            }
                        });
                        prev = ai;
                    }
                }
            } finally {
                directoryReader.close();
                executorService.shutdown();
                executorService.awaitTermination(30L, TimeUnit.MINUTES);
                workerService.shutdown();
                workerService.awaitTermination(60L, TimeUnit.MINUTES);
            }
        } finally {
            centralContext.releaseIndexSearcher(s);
        }

    }

    private static void fetchDir(final int index, final int total,
                                 final String subDir, ThreadPoolExecutor workerService) throws Exception {
        String content = httpGetText(buildCentralUrl(subDir));
        List<String> linkList = new ArrayList<String>();
        final String linkRegex = "<a href=\"(.*?)\".*>(.*?)<\\/a>";
        final Pattern linkPattern = Pattern.compile(linkRegex);
        Matcher matcher = linkPattern.matcher(content);
        while (matcher.find()) {
            String href = matcher.group(1);
            String title = matcher.group(2);
            linkList.add(href);
        }
        for (final String artifactUrl : linkList) {
            if (artifactUrl != null && !artifactUrl.startsWith("..") && !artifactUrl.endsWith("/")) {
                final String pkgUrl = buildNexusUrl(subDir + "/" + artifactUrl);
                workerService.submit(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        try {
                            if (!existFile(pkgUrl)) {
                                downloadFile(pkgUrl);
                                System.out.println(String.format("[%d/%d] successfully downloaded: %s", index, total, pkgUrl));
                            } else {
                                System.out.println(String.format("[%d/%d] existed: %s", index, total, pkgUrl));
                            }
                            return true;
                        } catch (Exception e) {
                            System.out.println(String.format("[%d/%d] failed to download: %s error: %s", index, total, pkgUrl, e.toString()));
                            return false;
                        }
                    }
                });
            }
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
        RequestConfig.Builder builder = RequestConfig.custom()
                .setConnectionRequestTimeout(timeoutInSeconds * 1000)
                .setSocketTimeout(timeoutInSeconds * 1000);
        requestConfig = builder.build();
        HttpGet request = new HttpGet(url);
        request.setConfig(requestConfig);
        request.setHeader("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36");
        return request;
    }

    private static String buildCentralUrl(String relativePath) {
        return String.format("%s%s", centralUrl, relativePath);
    }

    private static String buildNexusUrl(String relativePath) {
        return String.format("%s%s", nexusUrl, relativePath);
    }

    private static void filterDirectory( final Directory directory, final DocumentFilter filter )
        throws IOException
    {
        IndexReader r = null;
        IndexWriter w = null;
        try
        {
            r = DirectoryReader.open( directory );
            w = new NexusIndexWriter( directory, new NexusAnalyzer(), false );
            
            Bits liveDocs = MultiFields.getLiveDocs( r );

            int numDocs = r.maxDoc();

            for ( int i = 0; i < numDocs; i++ )
            {
                if ( liveDocs != null && !liveDocs.get( i ) )
                {
                    continue;
                }

                Document d = r.document( i );

                if ( !filter.accept( d ) )
                {
                    boolean success = w.tryDeleteDocument( r, i );
                    // FIXME handle deletion failure
                }
            }
            w.commit();
        }
        finally
        {
            IndexUtils.close( r );
            IndexUtils.close( w );
        }

        w = null;
        try
        {
            // analyzer is unimportant, since we are not adding/searching to/on index, only reading/deleting
            w = new NexusIndexWriter( directory, new NexusAnalyzer(), false );

            w.commit();
        }
        finally
        {
            IndexUtils.close( w );
        }
    }

    private Properties loadIndexProperties( final File indexDirectoryFile, final String remoteIndexPropertiesName )
    {
        File indexProperties = new File( indexDirectoryFile, remoteIndexPropertiesName );

        try ( FileInputStream fis = new FileInputStream( indexProperties ) )
        {
            Properties properties = new Properties();

            properties.load( fis );

            return properties;
        }
        catch ( IOException e )
        {
            getLogger().debug( "Unable to read remote properties stored locally", e );
        }
        return null;
    }

    private void storeIndexProperties( final File dir, final String indexPropertiesName, final Properties properties )
        throws IOException
    {
        File file = new File( dir, indexPropertiesName );

        if ( properties != null )
        {
            try ( OutputStream os = new BufferedOutputStream( new FileOutputStream( file ) ) )
            {
                properties.store( os, null );
            }
        }
        else
        {
            file.delete();
        }
    }

    private Properties downloadIndexProperties( final ResourceFetcher fetcher )
        throws IOException
    {
        try ( InputStream fis = fetcher.retrieve( IndexingContext.INDEX_REMOTE_PROPERTIES_FILE ) )
        {
            Properties properties = new Properties();

            properties.load( fis );

            return properties;
        }
    }

    public Date getTimestamp( final Properties properties, final String key )
    {
        String indexTimestamp = properties.getProperty( key );

        if ( indexTimestamp != null )
        {
            try
            {
                SimpleDateFormat df = new SimpleDateFormat( IndexingContext.INDEX_TIME_FORMAT );
                df.setTimeZone( TimeZone.getTimeZone( "GMT" ) );
                return df.parse( indexTimestamp );
            }
            catch ( ParseException ex )
            {
            }
        }
        return null;
    }

    /**
     * Unpack index data using specified Lucene Index writer
     * 
     * @param is an input stream to unpack index data from
     * @param d Directory for initialize indexWriter w
     * @param context An Indexing context is a statefull component, it keeps state of index readers and writers.
     */
    public static IndexDataReadResult unpackIndexData( final InputStream is, final Directory d,
                                                       final IndexingContext context )
        throws IOException
    {
        NexusIndexWriter w = new NexusIndexWriter( d, new NexusAnalyzer(), true );
        try
        {
            IndexDataReader dr = new IndexDataReader( is );

            return dr.readIndex( w, context );
        }
        finally
        {
            IndexUtils.close( w );
        }
    }

    /**
     * Filesystem-based ResourceFetcher implementation
     */
    public static class FileFetcher
        implements ResourceFetcher
    {
        private final File basedir;

        public FileFetcher( File basedir )
        {
            this.basedir = basedir;
        }

        public void connect( String id, String url )
            throws IOException
        {
            // don't need to do anything
        }

        public void disconnect()
            throws IOException
        {
            // don't need to do anything
        }

        public void retrieve( String name, File targetFile )
            throws IOException, FileNotFoundException
        {
            FileUtils.copyFile( getFile( name ), targetFile );

        }

        public InputStream retrieve( String name )
            throws IOException, FileNotFoundException
        {
            return new FileInputStream( getFile( name ) );
        }

        private File getFile( String name )
        {
            return new File( basedir, name );
        }

    }

    private abstract class IndexAdaptor
    {
        protected final File dir;

        protected Properties properties;

        protected IndexAdaptor( File dir )
        {
            this.dir = dir;
        }

        public abstract Properties getProperties();

        public abstract void storeProperties()
            throws IOException;

        public abstract void addIndexChunk( ResourceFetcher source, String filename )
            throws IOException;

        public abstract Date setIndexFile( ResourceFetcher source, String string )
            throws IOException;

        public Properties setProperties( ResourceFetcher source )
            throws IOException
        {
            this.properties = downloadIndexProperties( source );
            return properties;
        }

        public abstract Date getTimestamp();

        public void commit()
            throws IOException
        {
            storeProperties();
        }
    }

    private class LuceneIndexAdaptor
        extends IndexAdaptor
    {
        private final IndexUpdateRequest updateRequest;

        LuceneIndexAdaptor( IndexUpdateRequest updateRequest )
        {
            super( updateRequest.getIndexingContext().getIndexDirectoryFile() );
            this.updateRequest = updateRequest;
        }

        public Properties getProperties()
        {
            if ( properties == null )
            {
                properties = loadIndexProperties( dir, IndexingContext.INDEX_UPDATER_PROPERTIES_FILE );
            }
            return properties;
        }

        public void storeProperties()
            throws IOException
        {
            storeIndexProperties( dir, IndexingContext.INDEX_UPDATER_PROPERTIES_FILE, properties );
        }

        public Date getTimestamp()
        {
            return updateRequest.getIndexingContext().getTimestamp();
        }

        public void addIndexChunk( ResourceFetcher source, String filename )
            throws IOException
        {
            loadIndexDirectory( updateRequest, source, true, filename );
        }

        public Date setIndexFile( ResourceFetcher source, String filename )
            throws IOException
        {
            return loadIndexDirectory( updateRequest, source, false, filename );
        }

        public void commit()
            throws IOException
        {
            super.commit();

            updateRequest.getIndexingContext().commit();
        }

    }

    private class LocalCacheIndexAdaptor
        extends IndexAdaptor
    {
        private static final String CHUNKS_FILENAME = "chunks.lst";

        private static final String CHUNKS_FILE_ENCODING = "UTF-8";

        private final IndexUpdateResult result;

        private final ArrayList<String> newChunks = new ArrayList<String>();

        LocalCacheIndexAdaptor( File dir, IndexUpdateResult result )
        {
            super( dir );
            this.result = result;
        }

        public Properties getProperties()
        {
            if ( properties == null )
            {
                properties = loadIndexProperties( dir, IndexingContext.INDEX_REMOTE_PROPERTIES_FILE );
            }
            return properties;
        }

        public void storeProperties()
            throws IOException
        {
            storeIndexProperties( dir, IndexingContext.INDEX_REMOTE_PROPERTIES_FILE, properties );
        }

        public Date getTimestamp()
        {
            Properties properties = getProperties();
            if ( properties == null )
            {
                return null;
            }

            Date timestamp = DefaultIndexUpdater.this.getTimestamp( properties, IndexingContext.INDEX_TIMESTAMP );

            if ( timestamp == null )
            {
                timestamp = DefaultIndexUpdater.this.getTimestamp( properties, IndexingContext.INDEX_LEGACY_TIMESTAMP );
            }

            return timestamp;
        }

        public void addIndexChunk( ResourceFetcher source, String filename )
            throws IOException
        {
            File chunk = new File( dir, filename );
            FileUtils.copyStreamToFile( new RawInputStreamFacade( source.retrieve( filename ) ), chunk );
            newChunks.add( filename );
        }

        public Date setIndexFile( ResourceFetcher source, String filename )
            throws IOException
        {
            cleanCacheDirectory( dir );

            result.setFullUpdate( true );

            File target = new File( dir, filename );
            FileUtils.copyStreamToFile( new RawInputStreamFacade( source.retrieve( filename ) ), target );

            return null;
        }

        @Override
        public void commit()
            throws IOException
        {
            File chunksFile = new File( dir, CHUNKS_FILENAME );
            try ( BufferedOutputStream os = new BufferedOutputStream( new FileOutputStream( chunksFile, true ) ); //
                            Writer w = new OutputStreamWriter( os, CHUNKS_FILE_ENCODING ) )
            {
                for ( String filename : newChunks )
                {
                    w.write( filename + "\n" );
                }
                w.flush();
            }
            super.commit();
        }

        public List<String> getChunks()
            throws IOException
        {
            ArrayList<String> chunks = new ArrayList<String>();

            File chunksFile = new File( dir, CHUNKS_FILENAME );
            try ( BufferedReader r =
                new BufferedReader( new InputStreamReader( new FileInputStream( chunksFile ), CHUNKS_FILE_ENCODING ) ) )
            {
                String str;
                while ( ( str = r.readLine() ) != null )
                {
                    chunks.add( str );
                }
            }
            return chunks;
        }

        public ResourceFetcher getFetcher()
        {
            return new LocalIndexCacheFetcher( dir )
            {
                @Override
                public List<String> getChunks()
                    throws IOException
                {
                    return LocalCacheIndexAdaptor.this.getChunks();
                }
            };
        }
    }

    abstract static class LocalIndexCacheFetcher
        extends FileFetcher
    {
        LocalIndexCacheFetcher( File basedir )
        {
            super( basedir );
        }

        public abstract List<String> getChunks()
            throws IOException;
    }

    private IndexUpdateResult fetchAndUpdateIndex( final IndexUpdateRequest updateRequest, ResourceFetcher source,
                                      IndexAdaptor target )
        throws IOException
    {
        IndexUpdateResult result = new IndexUpdateResult();
        
        if ( !updateRequest.isForceFullUpdate() )
        {
            getLogger().info("Incremental Updated");
            Properties localProperties = target.getProperties();
            Date localTimestamp = null;

            if ( localProperties != null )
            {
                localTimestamp = getTimestamp( localProperties, IndexingContext.INDEX_TIMESTAMP );
            }

            // this will download and store properties in the target, so next run
            // target.getProperties() will retrieve it
            Properties remoteProperties = target.setProperties( source );

            Date updateTimestamp = getTimestamp( remoteProperties, IndexingContext.INDEX_TIMESTAMP );

            // If new timestamp is missing, dont bother checking incremental, we have an old file
            if ( updateTimestamp != null )
            {
                List<String> filenames =
                    incrementalHandler.loadRemoteIncrementalUpdates( updateRequest, localProperties, remoteProperties );

                // if we have some incremental files, merge them in
                if ( filenames != null )
                {
                    for ( String filename : filenames )
                    {
                        target.addIndexChunk( source, filename );
                    }

                    result.setTimestamp( updateTimestamp );
                    result.setSuccessful( true );
                    return result;
                }
            }
            else
            {
                updateTimestamp = getTimestamp( remoteProperties, IndexingContext.INDEX_LEGACY_TIMESTAMP );
            }

            // fallback to timestamp comparison, but try with one coming from local properties, and if not possible (is
            // null)
            // fallback to context timestamp
            if ( localTimestamp != null )
            {
                // if we have localTimestamp
                // if incremental can't be done for whatever reason, simply use old logic of
                // checking the timestamp, if the same, nothing to do
                if ( updateTimestamp != null && localTimestamp != null && !updateTimestamp.after( localTimestamp ) )
                {
                    //Index is up to date
                    result.setSuccessful( true );
                    return result;
                }
            }
        }
        else
        {
            // create index properties during forced full index download
            target.setProperties( source );
        }

        if ( !updateRequest.isIncrementalOnly() )
        {
            getLogger().info("Full Updated");
            Date timestamp = null;
            try
            {
                timestamp = target.setIndexFile( source, IndexingContext.INDEX_FILE_PREFIX + ".gz" );
                if ( source instanceof LocalIndexCacheFetcher )
                {
                    // local cache has inverse organization compared to remote indexes,
                    // i.e. initial index file and delta chunks to apply on top of it
                    for ( String filename : ( (LocalIndexCacheFetcher) source ).getChunks() )
                    {
                        target.addIndexChunk( source, filename );
                    }
                }
            }
            catch ( IOException ex )
            {
                // try to look for legacy index transfer format
                try
                {
                    timestamp = target.setIndexFile( source, IndexingContext.INDEX_FILE_PREFIX + ".zip" );
                }
                catch ( IOException ex2 )
                {
                    getLogger().error( "Fallback to *.zip also failed: " + ex2 ); // do not bother with stack trace
                    
                    throw ex; // original exception more likely to be interesting
                }
            }
            
            result.setTimestamp( timestamp );
            result.setSuccessful( true );
            result.setFullUpdate( true );
        }
        
        return result;
    }

    /**
     * Cleans specified cache directory. If present, Locker.LOCK_FILE will not be deleted.
     */
    protected void cleanCacheDirectory( File dir )
        throws IOException
    {
        File[] members = dir.listFiles();
        if ( members == null )
        {
            return;
        }

        for ( File member : members )
        {
            if ( !Locker.LOCK_FILE.equals( member.getName() ) )
            {
                FileUtils.forceDelete( member );
            }
        }
    }

}
