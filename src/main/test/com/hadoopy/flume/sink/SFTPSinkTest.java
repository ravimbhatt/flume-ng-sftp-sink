package com.hadoopy.flume.sink;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.sshd.SshServer;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.common.Session;
import org.apache.sshd.common.file.FileSystemView;
import org.apache.sshd.common.file.nativefs.NativeFileSystemFactory;
import org.apache.sshd.common.file.nativefs.NativeFileSystemView;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.PasswordAuthenticator;
import org.apache.sshd.server.command.ScpCommandFactory;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.server.sftp.SftpSubsystem;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.vngx.jsch.ChannelSftp;
import org.vngx.jsch.ChannelType;
import org.vngx.jsch.JSch;
import org.vngx.jsch.config.SessionConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Unit tests for SFTP sink.
 */

public class SFTPSinkTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder(new File("/tmp/"));

    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String SFTPDATA = "sftpdata";
    private static final String EVENTS_CSV = "events.csv";

    @Before
    public void prepare() throws IOException {
        setupSSHServer();
    }

    private void setupSSHServer() throws IOException {
        SshServer sshd;

        sshd = SshServer.setUpDefaultServer();
        sshd.setFileSystemFactory(new NativeFileSystemFactory() {
            @Override
            public FileSystemView createFileSystemView(final Session session) {
                return new NativeFileSystemView(session.getUsername(), false) {
                    @Override
                    public String getVirtualUserDir() {
                        return testFolder.getRoot().getAbsolutePath();
                    }
                };
            }

            ;
        });
        sshd.setPort(8001);
        sshd.setSubsystemFactories(Arrays.<NamedFactory<Command>>asList(new SftpSubsystem.Factory()));
        sshd.setCommandFactory(new ScpCommandFactory());
        sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider(testFolder.newFile("hostkey.ser").getAbsolutePath()));
        sshd.setPasswordAuthenticator(new PasswordAuthenticator() {
            @Override
            public boolean authenticate(final String username, final String password, final ServerSession session) {
                return StringUtils.equals(username, USERNAME) && StringUtils.equals(password, PASSWORD);
            }
        });
        sshd.start();
    }

    private String getFileContentServer(final String filename) throws Exception {
        SessionConfig config = new SessionConfig();
        config.setProperty(SessionConfig.STRICT_HOST_KEY_CHECKING, "no");
        org.vngx.jsch.Session session = JSch.getInstance().createSession(USERNAME, "localhost", 8001, config);

        session.connect(PASSWORD.getBytes(StandardCharsets.UTF_8));
        ChannelSftp sftpChannel = session.openChannel(ChannelType.SFTP);
        sftpChannel.connect();

        String fileContent = "";

        try {
            InputStream is = sftpChannel.get(filename);
            StringWriter writer = new StringWriter();
            IOUtils.copy(is, writer, "UTF-8");
            fileContent = writer.toString();
        } catch (IOException e) {
            if (e.getMessage().startsWith("Invalid status response")) {
                //all good, file not found, we return empty string as content
            } else {
                throw e;
            }
        } finally {
            sftpChannel.disconnect();
            session.disconnect();
        }

        return fileContent;
    }

    private Context prepareDefaultContext() { // Prepares a default context with Kafka Server Properties
        Context context = new Context();
        context.put("conn.host", "localhost");
        context.put("conn.port", "8001");
        context.put("conn.username", USERNAME);
        context.put("conn.password", PASSWORD);
        context.put("destinationFolder", SFTPDATA);
        context.put("destinationFile", EVENTS_CSV);
        context.put("rollCount", "1");

        return context;
    }

    @Test
    public void testFileCreation() throws Exception {
        Sink sftpSink = new SFTPSink();
        Context context = prepareDefaultContext();
        Configurables.configure(sftpSink, context);
        Channel memoryChannel = new MemoryChannel();
        Configurables.configure(memoryChannel, context);
        sftpSink.setChannel(memoryChannel);
        sftpSink.start();

        String msg = "first-message";
        Transaction tx = memoryChannel.getTransaction();
        tx.begin();
        Event event = EventBuilder.withBody(msg.getBytes());
        memoryChannel.put(event);
        tx.commit();
        tx.close();

        try {
            Sink.Status status = sftpSink.process();
            if (status == Sink.Status.BACKOFF) {
                fail("Error Occurred");
            }
        } catch (EventDeliveryException ex) {
            // ignore
        }

        String fileContent = getFileContentServer(SFTPDATA + File.separator + "0_" + EVENTS_CSV);
        assertEquals(fileContent, "first-message");
    }

    @Test
    public void testMultipleFileCreation() throws Exception {
        Sink sftpSink = new SFTPSink();
        Context context = prepareDefaultContext();
        Configurables.configure(sftpSink, context);
        Channel memoryChannel = new MemoryChannel();
        Configurables.configure(memoryChannel, context);
        sftpSink.setChannel(memoryChannel);
        sftpSink.start();

        Transaction tx = memoryChannel.getTransaction();
        tx.begin();
        String msg = "first-message";
        Event event = EventBuilder.withBody(msg.getBytes());
        memoryChannel.put(event);

        String msg2 = "second-message";
        Event event2 = EventBuilder.withBody(msg2.getBytes());
        memoryChannel.put(event2);

        tx.commit();
        tx.close();

        try {
            Sink.Status status = sftpSink.process();
            if (status == Sink.Status.BACKOFF) {
                fail("Error Occurred");
            }
            status = sftpSink.process();
            if (status == Sink.Status.BACKOFF) {
                fail("Error Occurred");
            }
        } catch (EventDeliveryException ex) {
            // ignore
        }

        String fileContent = getFileContentServer(SFTPDATA + File.separator + "0_" + EVENTS_CSV);
        assertEquals(fileContent, "first-message");

        String fileContent2 = getFileContentServer(SFTPDATA + File.separator + "1_" + EVENTS_CSV);
        assertEquals(fileContent2, "second-message");
    }

    @Test
    public void testFileCreationWithRollCount() throws Exception {
        Sink sftpSink = new SFTPSink();
        Context context = prepareDefaultContext();
        context.put("rollCount", "2");
        Configurables.configure(sftpSink, context);
        Channel memoryChannel = new MemoryChannel();
        Configurables.configure(memoryChannel, context);
        sftpSink.setChannel(memoryChannel);
        sftpSink.start();

        Transaction tx = memoryChannel.getTransaction();
        tx.begin();
        String msg = "first-message";
        Event event = EventBuilder.withBody(msg.getBytes());
        memoryChannel.put(event);

        String msg2 = "second-message";
        Event event2 = EventBuilder.withBody(msg2.getBytes());
        memoryChannel.put(event2);

        tx.commit();
        tx.close();

        try {
            Sink.Status status = sftpSink.process();
            if (status == Sink.Status.BACKOFF) {
                fail("Error Occurred");
            }
            status = sftpSink.process();
            if (status == Sink.Status.BACKOFF) {
                fail("Error Occurred");
            }
        } catch (EventDeliveryException ex) {
            // ignore
        }

        String fileContent = getFileContentServer(SFTPDATA + File.separator + "0_" + EVENTS_CSV);
        assertEquals(fileContent, "first-message\nsecond-message\n");

        String fileContent2 = getFileContentServer(SFTPDATA + File.separator + "1_" + EVENTS_CSV);
        assertEquals(fileContent2, "");
    }
}
