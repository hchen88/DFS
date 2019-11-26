import java.rmi.*;
import java.net.*;
import java.util.*;
import java.io.*;
import java.nio.file.*;
import java.math.BigInteger;
import java.security.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

//import server.comm.UDPServer;

import java.io.InputStream;
import java.util.*;


/* JSON Format

{"file":
  [
     {"name":"MyFile",
      "size":128000000,
      "pages":
      [
         {
            "guid":11,
            "size":64000000
         },
         {
            "guid":13,
            "size":64000000
         }
      ]
      }
   ]
} 
*/


public class DFS
{

    public class PagesJson
    {
//        Long guid;
        ArrayList<Long> guidList = null;
        Long size;

        public PagesJson(Long pageGuid, Long size)
        {
            this.guidList = new ArrayList<Long>();
            guidList.add(pageGuid);
            this.size = size;

        }
        // getters
        public ArrayList<Long> getGuidList(){return this.guidList; }

        public Long getSize() {
            return this.size;
        }



        // setters
        public void setGuidList(ArrayList<Long> guidList) {
            this.guidList = guidList;
        }

        public void setSize(Long size) {
            this.size = size;
        }

    };

    public class FileJson 
    {
        String name;
        Long   size;
        ArrayList<PagesJson> pages;
        public FileJson()
        {
            name = null;
            size = new Long(0);
            pages = new ArrayList<PagesJson>();
        }

        public FileJson(String name){
            this.name = name;
            size = new Long(0);
            pages = new ArrayList<PagesJson>();

        }

        // getters

        public String getName(){
            return this.name;
        }

        public Long getSize() {
            return this.size;
        }
        public ArrayList<PagesJson> getPages()
        {
        	return this.pages;
        }


        // setters
        public void setName(String name) {
            this.name = name;
        }

        public void setSize(Long size) {
            this.size = size;
        }

        public void appendPage(PagesJson page){
            pages.add(page);
            size += page.getSize();
        }

        public Boolean checkPage(Long guid) {
            Iterator<PagesJson> iterator = pages.iterator();
            while(iterator.hasNext()){
                if(iterator.next().guidList.get(0) == guid) {
                    return true;
                }
            }
            return false;
        }


    };
    
    public class FilesJson 
    {
         List<FileJson> file;
         public FilesJson() 
         {
             file = new ArrayList<FileJson>();
         }

         public FilesJson(FileJson fileJson) {
             file = new ArrayList<FileJson>();
             file.add(fileJson);
         }

         public void addFile(FileJson fileJson){
             file.add(fileJson);

         }
        // getters
         public List<FileJson> getFile()
         {
        	 return file;
         }
        // setters
        public void removeFile(String name){
             int indexToDelete = 0;
             for(int i = 0; i < file.size(); i++){
                 if(file.get(i).getName().equals(name)){
                     indexToDelete = i;
                 }
             }
             file.remove(indexToDelete);
        }
    };
    
    
    int port;
    Chord  chord;
    
    
    private long md5(String objectName)
    {
        try
        {
            MessageDigest m = MessageDigest.getInstance("MD5");
            m.reset();
            m.update(objectName.getBytes());
            BigInteger bigInt = new BigInteger(1,m.digest());
            return Math.abs(bigInt.longValue());
        }
        catch(NoSuchAlgorithmException e)
        {
                e.printStackTrace();
                
        }
        return 0;
    }
    
    
    
    public DFS(int port) throws Exception
    {
        this.port = port;
        long guid = md5("" + port);
        chord = new Chord(port, guid);
        Files.createDirectories(Paths.get(guid+"/repository"));
        Files.createDirectories(Paths.get(guid+"/tmp"));
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                chord.leave();
            }
        });
//        if(port == 2000) {
//        	UDPServer server = new UDPServer(this);
//        	server.start();
//        }
    }

/**
 * Join the chord
  *
 */
    public void join(String Ip, int port) throws Exception
    {
        chord.joinRing(Ip, port);
        chord.print();
    }
    
    
   /**
 * leave the chord
  *
 */ 
    public void leave() throws Exception
    {        
       chord.leave();
    }
  
   /**
 * print the status of the peer in the chord
  *
 */
    public void print() throws Exception
    {
        chord.print();
    }
    
    public Chord getChord()
    {
    	return chord;
    }
/**
 * readMetaData read the metadata from the chord
  *
 */
    public FilesJson readMetaData() throws Exception
    {
        FilesJson filesJson = null;
        try {
            Gson gson = new Gson();
            long guid = md5("Metadata");

           // System.out.println("GUID " + guid);
            ChordMessageInterface peer = chord.locateSuccessor(guid);
            RemoteInputFileStream metadataraw = peer.get(guid);
            metadataraw.connect();
            Scanner scan = new Scanner(metadataraw);
            scan.useDelimiter("\\A");
            String strMetaData = scan.next();
           // System.out.println(strMetaData);
            filesJson= gson.fromJson(strMetaData, FilesJson.class);
        } catch (Exception ex)
        {
            filesJson = new FilesJson();
        }
        return filesJson;
    }
    
/**
 * writeMetaData write the metadata back to the chord
  *
 */
    public void writeMetaData(FilesJson filesJson) throws Exception
    {
        long guid = md5("Metadata");
        ChordMessageInterface peer = chord.locateSuccessor(guid);
        
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        peer.put(guid, gson.toJson(filesJson));
    }
   
/**
 * Change Name
  *
 */
    public void move(String oldName, String newName) throws Exception
    {
        FileJson fileToMove = null;
        FilesJson files = readMetaData();
        for( FileJson file : files.file) {
            if(file.getName().equals(oldName)){
                fileToMove = file;
            }
        }
        fileToMove.setName(newName);
        writeMetaData(files);
        System.out.println("Changed filename from " + oldName + " to " + newName);

    }

  
/**
 * List the files in the system
  *
 */
    public String lists() throws Exception
    {
        FilesJson fileJson = readMetaData();
        String listOfFiles = "";
        for(int i = 0; i < fileJson.file.size(); i++){
            System.out.println("FileName: " + fileJson.file.get(i).getName());
            for(int j = 0; j < fileJson.file.get(i).pages.size(); j++){
                System.out.println("Page " + (j+1) + ": " + fileJson.file.get(i).pages.get(j).guidList.get(0));
                if(fileJson.file.get(i).pages.get(j).getGuidList().size() > 1) {
                    for (int l = 1; l < fileJson.file.get(i).pages.get(j).getGuidList().size(); l++) {
                        System.out.println("Page Guids: " + (fileJson.file.get(i).pages.get(j).getGuidList().get(l)));
                    }
                }
            }
        }
 
        return listOfFiles;
    }

/**
 * create an empty file 
  *
 * @param fileName Name of the file
 */
    public void create(String fileName) throws Exception
    {
        FilesJson files = readMetaData();
        FileJson fileJson = new FileJson(fileName);

        if(files.file == null) {
            FilesJson filesJson = new FilesJson(fileJson);
            writeMetaData(filesJson);

        } else{
            if(files.file.size() == 0) {
                files = new FilesJson(fileJson);
            }else {
                files.addFile(fileJson);
            }
        }

        // Write Metadata
        writeMetaData(files);
        System.out.println(fileName + " created");

    }
    
/**
 * delete file 
  *
 * @param fileName Name of the file
 */
    public void delete(String fileName) throws Exception
    {
        FileJson fileToDelete = null;
        boolean fileFound = false;
        FilesJson files = readMetaData(); //gets copy of files Json object
        for( FileJson file : files.file) {
            if(file.getName().equals(fileName)){
                fileToDelete = file;
                fileFound = true;
            }
        }

        if(fileFound){
            for(int i = 0; i < fileToDelete.pages.size(); i++){
                for ( int j = 0; j < fileToDelete.pages.get(i).guidList.size(); j++)
                {
                    Long chunkGuid = fileToDelete.pages.get(i).guidList.get(j);
                    ChordMessageInterface successor = chord.locateSuccessor(chunkGuid);
                    successor.delete(chunkGuid);
                    System.out.println(chunkGuid +" physically deleted!");
                }
                fileToDelete.pages.get(i).guidList.clear();
            }
            fileToDelete.pages.clear();
            System.out.println(fileToDelete.getName() +" deleted!");
            files.removeFile(fileToDelete.getName());
            writeMetaData(files);

        }else{
            System.out.println("Filename is incorrect!");
        }
        
    }
    
/**
 * Read block pageNumber of fileName 
  *
 * @param fileName Name of the file
 * @param pageNumber number of block. 
 */
    public RemoteInputFileStream read(String fileName, int pageNumber) throws Exception
    {
        FileJson fileToRead = null;
        FilesJson files = readMetaData();
        for( FileJson file : files.file) {
            if(file.getName().equals(fileName)){
                fileToRead = file;
            }
        }
        Long chunkGuid = fileToRead.pages.get(pageNumber).guidList.get(0);
        ChordMessageInterface successor = chord.locateSuccessor(chunkGuid);
        System.out.println("Page Guid:" + chunkGuid + " read.");
        return successor.get(chunkGuid);
    }
    
 /**
 * Add a page to the file                
  *
 * @param fileName Name of the file
 * @param data RemoteInputStream. 
 */
    public void append(String fileName, RemoteInputFileStream data) throws Exception
    {
        FileJson fileToAppend = null;
        FilesJson files = readMetaData(); //gets copy of files Json object
        for( FileJson file : files.file) {
            if(file.getName().equals(fileName)){
                fileToAppend = file;
            }
        }

        Long filePageGuid = md5(fileName + Integer.toString(fileToAppend.pages.size()));
        PagesJson page = new PagesJson(filePageGuid, (long)1000);
        if(!fileToAppend.checkPage(filePageGuid)) {
            fileToAppend.appendPage(page);
            writeMetaData(files);
            ChordMessageInterface successor = chord.locateSuccessor(filePageGuid);
            successor.put(filePageGuid, data);
            System.out.println(filePageGuid + " added to dfs!");
        }
    }

    /**
     * this method duplicates file 2 times and appends Guid into the guid list.
     * @param fileName - the filename as a string to duplicate
     * @param pageNumber - the page number as an int to specify which page with duplicate
     * @throws Exception
     */
    public void duplicate(String fileName, int pageNumber) throws Exception {
        FileJson fileToDup = null;
        RemoteInputFileStream data = null;
        ArrayList<Long> duplicationList = null;
        FilesJson files = readMetaData(); //gets copy of files Json object
        for( FileJson file : files.file) {
            if(file.getName().equals(fileName)){
                fileToDup = file;
            }
        }

        Long filePageGuid = fileToDup.getPages().get(pageNumber).getGuidList().get(0);
        ChordMessageInterface successor = chord.locateSuccessor(filePageGuid);
        Long directory = successor.getId();
        duplicationList = fileToDup.getPages().get(pageNumber).guidList;

        //duplicating 3 times
        for(int i = 0; i < 2; i++) {
            data = new RemoteInputFileStream(directory+"/repository/" + filePageGuid);
            Long duplicationGuid = md5(fileName + "Dupe" + i);
            ChordMessageInterface successor2 = chord.locateSuccessor(duplicationGuid);
            successor2.put(duplicationGuid, data);
            duplicationList.add(duplicationGuid);
            //need to add it to meta data.
            System.out.println(filePageGuid + " duplicated to dfs with Guid: " + duplicationGuid + "!");
        }
        writeMetaData(files); //write back to MetaData
    }


}
