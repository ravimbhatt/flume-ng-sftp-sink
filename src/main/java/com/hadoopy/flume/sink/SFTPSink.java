package com.hadoopy.flume.sink;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.UserInfo;

/**
 * A flume sink that writes to SFTP.
 */

public class SFTPSink extends AbstractSink implements Configurable {

    private static Logger LOG = LoggerFactory.getLogger(SFTPSink.class);

    /**
     * Configuration keys for flume config file.
     */
    private final static String CONN_PRIVATE_KEY_FILE = "conn.privatekeyfile";
    private final static String CONN_USERNAME = "conn.username";
    private final static String CONN_PASSWORD = "conn.password";
    private final static String CONN_HOST = "conn.host";
    private final static String CONN_PORT = "conn.port";
    private final static String CONN_KNOWN_HOSTS_FILE = "conn.knownhostsfile";
    //TODO: allow date based regular expression in folder name
    private final static String DESTINATION_FOLDER = "destinationFolder";
    private final static String DESTINATION_FILE = "destinationFile";
    private final static String ROLL_COUNT = "rollCount";
    //private final static String ROLL_SIZE="rollSize";
    private final static String TMP_DIR = "/tmp/";

    private String privateKeyFile;
    private String username;
    private String password;
    private String host;
    private int port;
    private String knownHosts;
    private String destinationFolder;
    private String destinationFileName;
    private long rollCount;
    //TODO: add a roll size to roll up the files based on size as well.
    //private int rollSize;

    private int consumedEventCount;
    private int fileCount;
    private Writer fileWriter;

    private Session session;
    private ChannelSftp channelSftp;

    @Override
    public void configure(Context context) {
        this.username = context.getString(CONN_USERNAME, System.getProperty("user.name"));
        this.password = context.getString(CONN_PASSWORD, "");
        this.host = context.getString(CONN_HOST, "localhost");
        this.port = context.getInteger(CONN_PORT, 22);

        if (this.password == "") {
            this.privateKeyFile = context.getString(CONN_PRIVATE_KEY_FILE, "/home/" + System.getProperty("user.name")
                                                                           + "/id_rsa");
            this.knownHosts = context.getString(CONN_KNOWN_HOSTS_FILE, "/home/" + System.getProperty("user.name")
                                                                       + "/known_hosts");
        }
        this.destinationFolder = context.getString(DESTINATION_FOLDER, "/home/" + this.username + "/");
        this.destinationFileName = context.getString(DESTINATION_FILE, "sftpsink.csv");
        this.rollCount = context.getLong(ROLL_COUNT, 10000L);
        //this.rollSize = context.getInteger(ROLL_SIZE,1000);
    }

    @Override
    public void start() {
        JSch jsch = new JSch();

        LOG.info("Attempting to connect to source via SFTP with" + " privateKey: " + privateKeyFile
                 + ", knownHosts: " + knownHosts + ", userName: " + username + ", hostName: " + host + ", port: " + port);

        try {
            if (privateKeyFile != null) {
                jsch.addIdentity(privateKeyFile);
            }

            session = jsch.getSession(username, host, port);

            if (privateKeyFile == null && password != null) {
                session.setPassword(password);
            }

            session.setConfig("PreferredAuthentications", "publickey,password");

            if (Strings.isNullOrEmpty(knownHosts)) {
                LOG.info("Known hosts path is not set, StrictHostKeyChecking will be turned off");
                session.setConfig("StrictHostKeyChecking", "no");
            } else {
                jsch.setKnownHosts(knownHosts);
            }

            if (!Strings.isNullOrEmpty(password)) {
                session.setPassword(password);
            }

            // Establish the connection
            UserInfo ui = new MyUserInfo();
            session.setUserInfo(ui);
            session.setDaemonThread(true);
            session.connect();

            // Open an SFTP channel
            channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();

            LOG.info("Finished connecting to SSH server");

        } catch (Exception e) {
            LOG.info("Error: while establishing a connection to sftp server.");
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        try {
            channelSftp.disconnect();
        } catch (Exception exception) {
        }
        try {
            session.disconnect();
        } catch (Exception exception) {
        }
    }

    private void writeToLocalFile(String data) throws IOException {
        if (fileWriter == null) {
            String fileName = getTmpFileName();
            LOG.debug("creating new file {} ", fileName);
            fileWriter = new FileWriter(fileName);
        }

        fileWriter.write(data + "\n");
    }

    private String getTmpFileName() {
        return TMP_DIR + fileCount + "_" + destinationFileName;
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();

        try {
            txn.begin();
            Event event = channel.take();

            if (event != null) {
                String data = new String(event.getBody());
                LOG.info("Writing data: " + data);
                writeToLocalFile(data);
                //check if we have reached rollCount
                consumedEventCount++;
                LOG.info("event, roll: " + consumedEventCount + "," + rollCount);
                if (consumedEventCount >= rollCount) {
                    fileWriter.close();
                    fileWriter = null;
                    //send local file to sftp now.
                    try {
                        // Does the directory exist?
                        SftpATTRS attrs = null;
                        try {
                            attrs = channelSftp.stat(destinationFolder);
                        } catch (SftpException ignore) {
                        }

                        // Create the directory since it doesn't exist
                        if (attrs == null) {
                            channelSftp.mkdir(destinationFolder);
                        }
                    } catch (SftpException e) {
                        throw new IOException("Failed to create remote directory over SFTP: " + destinationFolder, e);
                    }

                    String destination = destinationFolder + File.separator + fileCount + "_" + destinationFileName;
                    LOG.info("Transferring file " + getTmpFileName() + " to " + destination);

                    InputStream is = new FileInputStream(getTmpFileName());
                    try {
                        channelSftp.put(is, destination);
                    } catch (SftpException e) {
                        throw new IOException("Failed to transfer file over SFTP", e);
                    }

                    consumedEventCount = 0;
                    fileCount++;
                }

                status = Status.READY;
            } else {
                status = Status.BACKOFF;
            }

            txn.commit();

        } catch (Exception eventException) {
            LOG.error("Error: " + eventException.toString());

            txn.rollback();
            txn.close();
            status = Status.BACKOFF;

            throw new EventDeliveryException(eventException);
        } finally {
            txn.close();
        }

        return status;
    }

    /**
     * Implementation of UserInfo class for JSch which allows for password-less login via keys
     */
    public static class MyUserInfo implements UserInfo {

        // The passphrase used to access the private key
        @Override
        public String getPassphrase() {
            return null;
        }

        // The password to login to the client server
        @Override
        public String getPassword() {
            return null;
        }

        @Override
        public boolean promptPassword(String message) {
            return true;
        }

        @Override
        public boolean promptPassphrase(String message) {
            return true;
        }

        @Override
        public boolean promptYesNo(String message) {
            return true;
        }

        @Override
        public void showMessage(String message) {
            LOG.info(message);
        }
    }

}
