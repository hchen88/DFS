import java.io.*;
import java.rmi.Remote;

import com.google.gson.*;
import com.google.gson.stream.*;




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

            if(result[0].equals("read")){
                dfs.read(result[1], Integer.parseInt(result[2]));
            }

            if(result[0].equals("path")){
                File file = new File("path.txt");
                System.out.println(file.getAbsolutePath());
                file.delete();
            }

            if(result[0].equals("duplicate")){
                dfs.duplicate(result[1], Integer.parseInt(result[2]));
            }


            line=buffer.readLine();
        }
            // User interface:
            // X join,  X ls,  X touch, X delete, read -return remoteInputFileStream as object, tail- last page, head - first page, X append, move

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
