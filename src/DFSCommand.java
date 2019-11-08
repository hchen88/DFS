
import java.io.*;
import java.rmi.Remote;
import java.security.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.google.gson.*;
import com.google.gson.stream.*;

import java.time.Instant;


public class DFSCommand
{
    DFS dfs;
        
    public DFSCommand(int p, int portToJoin) throws Exception {
        dfs = new DFS(p);
        
        if (portToJoin > 0)
        {
            System.out.println("Joining "+ portToJoin);
            dfs.join("127.0.0.1", portToJoin);            
        }
        
        BufferedReader buffer=new BufferedReader(new InputStreamReader(System.in));
        String line = buffer.readLine();  
        while (!line.equals("quit"))
        {
            String[] result = line.split("\\s");
            if(result[0].equals("timestamp")) {
                Date date = new Date();
                SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy h:mm:ss a");
                String formattedDate = sdf.format(date);
                System.out.println(formattedDate); // 12/01/2011 4:48:16 PM
            }

            if(result[0].equals("commands")) {
                System.out.print("ls - list of files\nprint - prints successor, predecessor, and fingers\njoin" +
                        " <portNumber> - joins ports\nleave - leave chord ring\ncreate <fileName> - creates empty file" +
                        " with no pages\nappend <fileName> <fileLocation> - adds pages/chunks to file\ndelete" +
                        " <fileName>- deletes file(including all pages associated)\nmove <oldFilename> <newFilename> -" +
                        " \nread - <fileName> <pageNumber> - reads page\nwrite - <fileName> <pageNumber> -writes page\n");

            }

            if (result[0].equals("join")  && result.length > 1)
            {
                dfs.join("127.0.0.1", Integer.parseInt(result[1]));     
            }
            if (result[0].equals("print"))
            {
                dfs.print();     
            }
            if (result[0].equals("ls"))
            {
                System.out.println(dfs.lists());
            }
            
            if (result[0].equals("leave"))
            {
                dfs.leave();     
            }

            if(result[0].equals("create")){
                dfs.create(result[1]);
            }

            if(result[0].equals("append")){
                //append
                dfs.append(result[1], new RemoteInputFileStream(result[2]));
            }

            if(result[0].equals("delete")) {
                dfs.delete(result[1]);

            }
            if(result[0].equals("move")) {
                dfs.move(result[1], result[2]);
            }
            line=buffer.readLine();

            if(result[0].equals("read")){
                dfs.read(result[1], Integer.parseInt(result[2]));
            }

        }

    }
    
    static public void main(String args[]) throws Exception
    {
        /**
        Gson gson = new Gson();
        RemoteInputFileStream in = new RemoteInputFileStream("music.json", false);
        in.connect();
        Reader targetReader = new InputStreamReader(in);
        JsonReader jreader = new  JsonReader(targetReader);
        Music[] music = gson.fromJson(jreader, Music[].class);
        **/

        if (args.length < 1 ) {
            throw new IllegalArgumentException("Parameter: <port> <portToJoin>");
        }
        if (args.length > 1 ) {
            DFSCommand dfsCommand=new DFSCommand(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
        }
        else
        {
            DFSCommand dfsCommand=new DFSCommand( Integer.parseInt(args[0]), 0);
        }
     } 
}
