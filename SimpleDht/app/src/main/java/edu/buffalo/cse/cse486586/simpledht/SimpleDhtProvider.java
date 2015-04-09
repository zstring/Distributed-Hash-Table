package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

public class SimpleDhtProvider extends ContentProvider {

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static final String JOIN = "join";
    private static final String NEWJOIN = "newjoin";
    private static final String MESSAGE = "message";
    private static final String PREDECESSOR = "predecessor";
    private static final String SUCCESSOR = "successor";
    private static final String FULLDATA = "\"*\"";
    private static final String SELFDATA = "\"@\"";
    private static final String DELETE = "delete";
    private static final String QUERY = "query";
    private static final String RESULT = "result";
    private static final String[] REMOTE_PORTS = {"11108", "11112", "11116", "11120", "11124"};
    private static final int SERVER_PORT = 10000;
    private static final int TOTAL_AVDS = 5;
    private static int deliveryCount = -1;
    private static int counter = 0;
    private static String myId;
    private static String myPort;
    private static final String URI_STRING = "edu.buffalo.cse.cse486586.simpledht.provider";
    private static ContentResolver mContentResolver;
    private static String ENTRY_NODE;
    private ServerTask serverTask;
    private static Context context;
    private Uri mUri;
    private static Node node;
    private static HashSet<ContentValues> bufferData = new HashSet<>();
    private static HashMap<String, MatrixCursor> hmResult = new HashMap<>();

    int poolSize = 10;
    int maxPoolSize = 20;
    int maxTime = 40;
    private BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(maxPoolSize);
    private Executor threadPoolExecutor = new ThreadPoolExecutor(poolSize, maxPoolSize, maxTime, TimeUnit.SECONDS, workQueue);

    private class Node {
        private String key;
        private String successor;
        private String predecessor;

        public Node(String key) {
            this.key = key;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getPredecessor() {
            return predecessor;
        }

        public void setPredecessor(String predecessor) {
            this.predecessor = predecessor;
        }

        public String getSuccessor() {
            return successor;
        }

        public void setSuccessor(String successor) {
            this.successor = successor;
        }
    }

    public SimpleDhtProvider() {
        Log.v("start", "WHO STARTING THIS EVENT");
    }

    private void sendJoiningMessage() {
        String[] msgToSend = {NEWJOIN, myPort};
        sendMessageToClient(msgToSend);
    }

    private void setUpServerListener() {
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            serverTask = new ServerTask();
            serverTask.executeOnExecutor(threadPoolExecutor, serverSocket);
        } catch (IOException ex) {
            Log.e("Error", "Error in setUpServerListener " + ex.getMessage());
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        FileOutputStream fos = null;
        String fileName = selection;
        if (SELFDATA.equals(fileName)) {
            deleteSelfData();
        }
        else if (FULLDATA.equals(fileName)) {
            deleteSelfData();
            String[] msgToSend = {"DELETE"};
            if (!ENTRY_NODE.equals(node.getSuccessor()))
                sendMessageToClient(msgToSend);
        } else {
            context.deleteFile(fileName);
        }
        Log.v("delete", fileName);
        return 0;
    }

    private void deleteSelfData() {
        String[] fileList = context.fileList();
        if (fileList != null) {
            for (String fileName : fileList) {
                context.deleteFile(fileName);
            }
            Log.v("delete", "ALL files DELTED");
        }
        else {
            Log.v("delete", "NO FILE TO DELETE");
        }
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String key = values.getAsString(KEY_FIELD);
        boolean flag = belongToSelf(key);
        if (flag) {
            insertData(values);
        } else {
            sendToSuccessorAvd(values);
        }
        return uri;
    }

    private void insertData(ContentValues values) {
        // if flag is true then insert the key value in this avd only
        FileOutputStream fos = null;
        String key = values.getAsString(KEY_FIELD);
        String value = values.getAsString(VALUE_FIELD);
        try {
            fos = context.openFileOutput(key, context.MODE_PRIVATE);
            fos.write(value.getBytes());
            fos.close();
            Log.v("insert", value.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendToSuccessorAvd(ContentValues values) {
        String[] msgToSend = {MESSAGE, values.getAsString(KEY_FIELD), values.getAsString(VALUE_FIELD)};
        sendMessageToClient(msgToSend);

        Log.v("message", "AFTER SENDIn The MSG TO nEXT AVD");
    }

    private void sendMessageToClient(String[] msgToSend) {
        AsyncTask<String, Void, Void> client =
                new ClientTask().executeOnExecutor(threadPoolExecutor, msgToSend);
    }

    private boolean belongToSelf(String key) {
        try {
            String hashed = genHash(key);
            if (node.getSuccessor() == null) return true;
            if (node.getKey().compareTo(node.getPredecessor()) < 0 &&
                    (hashed.compareTo(node.getKey()) <= 0 || hashed.compareTo(node.getPredecessor()) > 0)) {
                return true;
            } else if (hashed.compareTo(node.getKey()) <= 0 && hashed.compareTo(node.getPredecessor()) > 0) {
                // if hashed key is greater than the predecessor keys but less than or eqault o
                // self keys than the key belongs to its self
                return true;
            } else {
                return false;
            }
        } catch (NoSuchAlgorithmException ex) {
            Log.e("Error", " Error in belong To Self SimpleDhtProvider " + ex.getMessage());
        }
        return false;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        this.context = this.getContext();
        mUri = buildUri("content", URI_STRING);
        try {
            ENTRY_NODE = genHash("5554");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

                if (context == null) context = this.getContext();
        TelephonyManager tel = (TelephonyManager)context.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String myPort = String.valueOf((Integer.parseInt(portStr)));
        this.myId = myPort;
        this.myPort = myPort;
        mContentResolver = context.getContentResolver();
        try {
            node = new Node(genHash(myId));
        } catch (NoSuchAlgorithmException e) {
            Log.e("Error", " Error in Constructor SimpleDhtProvider " + e.getMessage());
        }
        setUpServerListener();
        if (!node.getKey().equals(ENTRY_NODE))
            sendJoiningMessage();
        return false;
    }

    //SelectionArgs[0] == Port ID who started the query process
    //SelectionARgs[1] == Unique ID of the query to identify the process.
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        String[] columns = {KEY_FIELD, VALUE_FIELD};
        MatrixCursor cr = new MatrixCursor(columns);
        if (SELFDATA.equals(selection)) {
            cr = getSelfData();
        } else if (FULLDATA.equals(selection)) {
            cr = getSelfData();
            MatrixCursor crOther = getDataFromOtherAVD(selection, selectionArgs);
            crOther.moveToFirst();
            int keyIndex = crOther.getColumnIndex(KEY_FIELD);
            int valueIndex = crOther.getColumnIndex(VALUE_FIELD);
            do {
                if (crOther.getCount() == 0) break;
                String[] row = new String[2];
                row[0] =  crOther.getString(keyIndex);
                row[1] = crOther.getString(valueIndex);
                cr.addRow(row);
            }while (crOther.moveToNext());
        } else {
            boolean flag = belongToSelf(selection);
            if (flag) {
                String valueContent = getValueFromKey(selection);
                if (!valueContent.isEmpty()) {
                    String[] row = new String[2];
                    row[0] = selection;
                    row[1] = valueContent;
                    cr.addRow(row);
                }
            } else {
                cr = getDataFromOtherAVD(selection, selectionArgs);
            }
            Log.v("query", selection);
        }
        return cr;
    }

    private MatrixCursor getDataFromOtherAVD(String selection, String[] selectionArgs) {
        String originPort = myPort;
        String uniqueId  = "";
        String[] columns = {KEY_FIELD, VALUE_FIELD};
        MatrixCursor mat = new MatrixCursor(columns);
        //if there is only onde node in the system

        if (node.getSuccessor() == null)
            return mat;
        if (selectionArgs != null) {
            originPort = selectionArgs[0];
            uniqueId = selectionArgs[1];
        } else {
            uniqueId = myPort + counter++;
        }
        try {
            if (!node.getSuccessor().equals(genHash(originPort))) {
                String[] msgToSend = {QUERY, selection, originPort, uniqueId};
                sendMessageToClient(msgToSend);
                    while (!hmResult.containsKey(uniqueId)) {
                        Thread.sleep(100);
                    }
                mat = hmResult.get(uniqueId);
            }
        } catch (NoSuchAlgorithmException e1) {
            e1.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return mat;
    }

    private MatrixCursor getSelfData() {
        String[] columns = {KEY_FIELD, VALUE_FIELD};
        MatrixCursor cr = new MatrixCursor(columns);
        String[] fileLists = context.fileList();
        if (fileLists != null) {
            for (String fileName : fileLists) {
                String valueContent = getValueFromKey(fileName);
                if (!valueContent.isEmpty()) {
                    String[] row = new String[2];
                    row[0] = fileName;
                    row[1] = valueContent;
                    cr.addRow(row);
                }
            }
            Log.v("query", "GETTING SELF DATA");
        }
        return  cr;
    }

    private String getValueFromKey(String fileName) {
        String valueContent = null;
        try{
            File fl = new File(fileName);
            FileInputStream fis = context.openFileInput(fileName);
            StringBuilder sb = new StringBuilder();
            int val = fis.read();
            while (val != -1) {
                sb.append((char) val);
                val = fis.read();
            }
            valueContent = sb.toString();
            fis.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return valueContent;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            while (true) {
                try {
                    Socket clientS = serverSocket.accept();
                    Date dStart = new Date();
                    InputStreamReader is = new InputStreamReader(clientS.getInputStream());
                    BufferedReader br = new BufferedReader(is);
                    String msg = br.readLine();
                    String type = msg.split(" ")[0];
                    if (JOIN.equals(type) || NEWJOIN.equals(type)) {
                        updateRingWithNewNode(msg.split(" ")[1]);
                    }
                    else if (MESSAGE.equals(type)) {
                        String key = msg.split(" ")[1];
                        String value = msg.split(" ")[2];
                        ContentValues cv = new ContentValues();
                        cv.put(KEY_FIELD, key);
                        cv.put(VALUE_FIELD, value);
                        insert(mUri, cv);
                    } else if (PREDECESSOR.equals(type)) {
                        updatePredecessorAndAddData(msg);
                    } else if (SUCCESSOR.equals(type)) {
                        updateSuccessor(msg);
                    } else if (QUERY.equals(type)) {
                        publishProgress(msg);
                    } else if (RESULT.equals(type)) {
                        updateResultMapObject(msg);
                    }
                    br.close();
                    is.close();
                    clientS.close();
                } catch (SocketTimeoutException ex) {
                    Log.e("Error: ", ex.getMessage() + " Server Catch Exception");
                } catch (IOException ex) {
                    Log.v("Error: ", ex.getMessage() + "  Server Catch Exception");
                }
            }
        }

        private void updateResultMapObject(String msg) {
            String[] data = msg.split(" ");
            String[] columns = {KEY_FIELD, VALUE_FIELD};
            MatrixCursor cr = new MatrixCursor(columns);
            for (int i = 2; i < data.length; i+=2) {
                //to remove any space values
                if (data[i].trim().isEmpty()) i++;
                String[] row = new String[2];
                row[0] = data[i];
                row[1] = data[i + 1];
                cr.addRow(row);
            }
            String uniqueId = data[1];
            hmResult.put(uniqueId, cr);
        }

        //msg format
        //msg[1] = KEY
        //msg[2] = originPort
        //msg[3] = uniqueID of the query
        private void fireQueryAndReturnResults(String msg) {
            String keyString = msg.split(" ")[1];
            String uniqueId = msg.split(" ")[3];
            String[] originPort = {msg.split(" ")[2], uniqueId};

            Cursor cr = query(mUri, null, keyString, originPort, null);
            int keyIndex = cr.getColumnIndex(KEY_FIELD);
            int valueIndex = cr.getColumnIndex(VALUE_FIELD);

            cr.moveToFirst();
            String keyValues = "";
            do {
                if (cr.getCount() != 0) {
                    String key = cr.getString(keyIndex);
                    String value = cr.getString(valueIndex);
                    keyValues += key + " " + value + " ";
                }
            }  while (cr.moveToNext());
            String[] msgToSend = {RESULT, uniqueId, keyValues};
            sendMessageToClient(msgToSend);
        }

        private void updateSuccessor(String msg) {
            try {
                node.setSuccessor(genHash(msg.split(" ")[1]));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

        private void updatePredecessorAndAddData(String msg) {
            try {
                node.setPredecessor(genHash(msg.split(" ")[1]));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

        private void updateRingWithNewNode(String newNodeId) {
            try {
                String hashed = genHash(newNodeId);
                String prevSuccessor = "";
                if (node.getKey().equals(ENTRY_NODE) &&
                        node.getPredecessor() == null) {
                    node.setPredecessor(hashed);
                    node.setSuccessor(hashed);
                    String[] msgToSendPre = {PREDECESSOR, newNodeId, myPort};
                    sendMessageToClient(msgToSendPre);
                    String[] msgToSendSuc = {SUCCESSOR, newNodeId, myPort};
                    sendMessageToClient(msgToSendSuc);
                    //removeAndSendKeyValuesToNewNode(newNodeId);
                } else {
                    if((hashed.compareTo(node.getKey()) <=0 && hashed.compareTo(node.getPredecessor()) > 0) ||
                            (node.getKey().compareTo(node.getPredecessor()) < 0 &&
                                    (hashed.compareTo(node.getKey()) <= 0 || hashed.compareTo(node.getPredecessor()) > 0))) {
                        node.setPredecessor(hashed);
                        String[] msgToSend = {SUCCESSOR, newNodeId, myPort};
                        sendMessageToClient(msgToSend);
//                        removeAndSendKeyValuesToNewNode(newNodeId);
                    }
                    else if ((hashed.compareTo(node.getKey()) > 0 && hashed.compareTo(node.getSuccessor()) < 0) ||
                                (node.getKey().compareTo(node.getSuccessor()) > 0 &&
                                        (hashed.compareTo(node.getKey()) > 0 || hashed.compareTo(node.getSuccessor()) < 0))) {

                        String[] msgToSend = {PREDECESSOR, newNodeId, myPort};
                        prevSuccessor = node.getSuccessor();
                        node.setSuccessor(hashed);
                        sendMessageToClient(msgToSend);
                    }
                    String entryNodeHashed = genHash(ENTRY_NODE);
                    if (prevSuccessor.isEmpty()) prevSuccessor = node.getSuccessor();
                    if (!node.getSuccessor().equals(entryNodeHashed) && !prevSuccessor.equals(hashed)) {
                        String[] updateRing = {JOIN, newNodeId, prevSuccessor};
                        sendMessageToClient(updateRing);
                    }
                }
            } catch (NoSuchAlgorithmException ex) {
                Log.e("Error", " Error in belong To Self SimpleDhtProvider " + ex.getMessage());
            }
        }

        // flag == 1 means updated predecessor'
        // flag == 2 means updated sucesssor
        private void removeAndSendKeyValuesToNewNode(String newNodeId) {
          Cursor mat = query(mUri, null, SELFDATA, null, null);
          int keyIndex = mat.getColumnIndex(KEY_FIELD);
          int valueIndex = mat.getColumnIndex(VALUE_FIELD);
          try {
              mat.moveToFirst();
              do{
                  String key = mat.getString(keyIndex);
                  String value = mat.getString(valueIndex);
                  String hashedKey = genHash(key);
                  if (node.getPredecessor() != null &&
                          hashedKey.compareTo(node.getPredecessor()) <= 0) {
                    String[] msgToSend = {"MESSAGE", key, value};
                    delete(mUri, key, null);
                      sendMessageToClient(msgToSend);
                  }
              } while (mat.moveToNext());
          } catch (NoSuchAlgorithmException e) {
              e.printStackTrace();
          }
        }

        protected void onProgressUpdate(String... strings) {
                fireQueryAndReturnResults(strings[0]);
//            String msg = strings[0].trim();
//            String order = strings[1].trim();
//
//            if (!order.equals("F"))
//                deliveryCount++;
////            TextView textView = (TextView) context.findViewById(R.id.textView1);
////            textView.append(deliveryCount + "\t" + order + "\t" + msg + "\n");
//            if (!order.equals("F")) {
//                ContentValues cv = new ContentValues();
//                cv.put(KEY_FIELD, deliveryCount + "");
//                cv.put(VALUE_FIELD, msg);
//                insert(mUri, cv);
//            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... params) {
            String type = params[0];
            if (NEWJOIN.equals(type) || JOIN.equals(type)) {
                String remotePort = null;
                if (JOIN.equals(type)) {
                    for (int i = 0; i < TOTAL_AVDS; i++) {
                        try {
                            String successor = params[2];
                            String remotePortHashed = genHash((Integer.parseInt(REMOTE_PORTS[i]) / 2) + "");
                            if (remotePortHashed.equals(successor)) {
                                remotePort = REMOTE_PORTS[i];
                                break;
                            }
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }
                    }
                } else if (NEWJOIN.equals(type)) {
                    remotePort = 5554 * 2 + "";
                }
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                    String msgToSend = JOIN + " " + params[1];
                    pw.println(msgToSend);
                    pw.flush();
                    pw.close();
                    socket.close();
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else if (MESSAGE.equals(type)) {
               sendMessage(params);
            } else if (DELETE.equals(type)) {
               sendMessage(params);
            } else if (QUERY.equals(type)) {
               sendMessage(params);
            } else if (PREDECESSOR.equals(type)) {
                sendMessage(params);
            } else if (SUCCESSOR.equals(type)) {
                sendMessage(params);
            } else if (RESULT.equals(type)) {
                sendMessage(params);
            }
            return null;
        }

        private void sendMessage(String[] params) {
            for (int i = 0; i < TOTAL_AVDS; i++) {
                try {
                    String remotePortHashed = genHash((Integer.parseInt(REMOTE_PORTS[i]) / 2) + "");
                    String comparePort = node.getSuccessor();
                    if (PREDECESSOR.equals(params[0]) || SUCCESSOR.equals(params[0])) {
                        comparePort = genHash(params[1]);
                    }
                    if (RESULT.equals(params[0])) {
                        comparePort = node.getPredecessor();
//                        comparePort = genHash("5554");
                    }
                    if (remotePortHashed.equals(comparePort)) {
                        String remotePort = REMOTE_PORTS[i];
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort));
                        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                        String msgToSend = null;
                        if (MESSAGE.equals(params[0])) {
                            msgToSend = params[0] + " " + params[1] + " " + params[2];
                        } else if (DELETE.equals(params[0])) {
                            msgToSend = params[0] + " " + params[2];
                        } else if (QUERY.equals(params[0])) {
                            //param = "type" + " " + "key" + " " + "originPort" + " " + "unqieID"
                            msgToSend = params[0] + " " + params[1] + " " + params[2] + " " + params[3];
                        } else if (PREDECESSOR.equals(params[0])) {
                            //param = "type" + "predecessor ID"
                            msgToSend = params[0] + " " + params[2];
                        } else if (SUCCESSOR.equals(params[0])) {
                            msgToSend = params[0] + " " + params[2];
                        } else if (RESULT.equals(params[0])) {
                            //params = "type" +" " + "uniqueID" + " " + "resultValue"
                            msgToSend = params[0] + " " + params[1] + " " + params[2];
                        }
                        pw.println(msgToSend.trim());
                        pw.flush();
                        pw.close();
                        socket.close();
                        break;
                    }
                } catch (SocketTimeoutException ex) {
                    Log.e("Error: AVD FAILED " + i + ", ", ex.getMessage());
                } catch (UnknownHostException ex) {
                    Log.e("Error: AVD FAILED " + i + ", ", ex.getMessage());
                } catch (IOException ex) {
                    Log.e("Error: AVD FAILED " + i + ", ", ex.getMessage());
                } catch (NoSuchAlgorithmException ex) {
                    Log.e("Error: AVD FAILED " + i + ", ", ex.getMessage());
                    ex.printStackTrace();
                }
            }
        }
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
}
