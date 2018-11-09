package org.apache.lucene.demo;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.lucene.analysis.es.SpanishAnalyzer;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.Date;


/** Index all text files under a directory.
 * <p>
 * This is a command-line application demonstrating simple Lucene indexing.
 * Run it with no command-line arguments for usage information.
 */
public class IndexFiles {
  
  private IndexFiles() {}

  /** Index all text files under a directory. 
 * @throws SAXException 
 * @throws ParserConfigurationException */
  public static void main(String[] args) throws SAXException, ParserConfigurationException {
    String usage = "java org.apache.lucene.demo.IndexFiles"
                 + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update]\n\n"
                 + "This indexes the documents in DOCS_PATH, creating a Lucene index"
                 + "in INDEX_PATH that can be searched with SearchFiles";
    String indexPath = "index";
    String docsPath = null;
    boolean create = true;
    for(int i=0;i<args.length;i++) {
      if ("-index".equals(args[i])) {
        indexPath = args[i+1];
        i++;
      } else if ("-docs".equals(args[i])) {
        docsPath = args[i+1];
        i++;
      } else if ("-update".equals(args[i])) {
        create = false;
      }
    }

    if (docsPath == null) {
      System.err.println("Usage: " + usage);
      System.exit(1);
    }

    final File docDir = new File(docsPath);
    if (!docDir.exists() || !docDir.canRead()) {
      System.out.println("Document directory '" +docDir.getAbsolutePath()+ "' does not exist or is not readable, please check the path");
      System.exit(1);
    }
    
    Date start = new Date();
    try {
      System.out.println("Indexing to directory '" + indexPath + "'...");

      Directory dir = FSDirectory.open(Paths.get(indexPath));
     /** Map<String,Analyzer> analyzerMap = new HashMap<String, Analyzer>();
      /*Aqui declaro el analizador a utilizar para cada etiqueta
      analyzerMap.put("title", new SpanishAnalyzer());
      analyzerMap.put("subject", new SpanishAnalyzer());
      analyzerMap.put("creator", new SpanishAnalyzer());
      analyzerMap.put("date", new SpanishAnalyzer());
      /*La etiqueta description tiene que ser dividida en dos, date2 y description /
      analyzerMap.put("description", new SpanishAnalyzer());
      Analyzer ana = CustomAnalyzer.builder(Paths.get("/path/to/config/dir"))
    		   .withTokenizer(StandardTokenizerFactory.class)
    		   .addTokenFilter(StandardFilterFactory.class)
    		   .addTokenFilter(LowerCaseFilterFactory.class)
      /*Probablemente spanish_stop.txt no sea la ruta correcta /
    		   .addTokenFilter(StopFilterFactory.class, "ignoreCase", "false", "words", "spanish_stop.txt", "format", "wordset")
      /*Aqui faltan por añadir mas filtros y es el mayor problema, ya que deben tokenizar solo los periodos de tiempo incluidos
       *en la descripcion, por ejemplo, "a partir de 1900" debe indexarse como 1900-2018 por lo que hay que usar expresiones
       *regulares o en su defecto, y de esto no estoy seguro, la libreria que nos digo OpeNLP, aunque su documentacion es bastante
       *confusa y no estoy seguro de que tenga un modulo de detector de fechas en español (en ingles si lo tiene) /
    		   .build();
      analyzerMap.put("date2", ana);
      PerFieldAnalyzerWrapper analyzer = new PerFieldAnalyzerWrapper(new SpanishAnalyzer(), analyzerMap);**/
      IndexWriterConfig iwc = new IndexWriterConfig(new SpanishAnalyzer());

      if (create) {
        // Create a new index in the directory, removing any
        // previously indexed documents:
        iwc.setOpenMode(OpenMode.CREATE);
      } else {
        // Add new documents to an existing index:
        iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
      }

      // Optional: for better indexing performance, if you
      // are indexing many documents, increase the RAM
      // buffer.  But if you do this, increase the max heap
      // size to the JVM (eg add -Xmx512m or -Xmx1g):
      //
      // iwc.setRAMBufferSizeMB(256.0);

      IndexWriter writer = new IndexWriter(dir, iwc);
      indexDocs(writer, docDir);

      // NOTE: if you want to maximize search performance,
      // you can optionally call forceMerge here.  This can be
      // a terribly costly operation, so generally it's only
      // worth it when your index is relatively static (ie
      // you're done adding documents to it):
      //
      // writer.forceMerge(1);

      writer.close();

      Date end = new Date();
      System.out.println(end.getTime() - start.getTime() + " total milliseconds");

    } catch (IOException e) {
      System.out.println(" caught a " + e.getClass() +
       "\n with message: " + e.getMessage());
    }
  }

  /**
   * Indexes the given file using the given writer, or if a directory is given,
   * recurses over files and directories found under the given directory.
   * 
   * NOTE: This method indexes one document per input file.  This is slow.  For good
   * throughput, put multiple documents into your input file(s).  An example of this is
   * in the benchmark module, which can create "line doc" files, one document per line,
   * using the
   * <a href="../../../../../contrib-benchmark/org/apache/lucene/benchmark/byTask/tasks/WriteLineDocTask.html"
   * >WriteLineDocTask</a>.
   *  
   * @param writer Writer to the index where the given file/dir info will be stored
   * @param file The file to index, or the directory to recurse into to find files to index
   * @throws IOException If there is a low-level I/O error
 * @throws SAXException 
 * @throws ParserConfigurationException 
   */
  static void indexDocs(IndexWriter writer, File file)
    throws IOException, SAXException, ParserConfigurationException {
    // do not try to index files that cannot be read
    if (file.canRead()) {
      if (file.isDirectory()) {
        String[] files = file.list();
        // an IO error could occur
        if (files != null) {
          for (int i = 0; i < files.length; i++) {
            indexDocs(writer, new File(file, files[i]));
          }
        }
      } else {

        FileInputStream fis;
        try {
          fis = new FileInputStream(file);
        } catch (FileNotFoundException fnfe) {
          // at least on windows, some temporary files raise this exception with an "access denied" message
          // checking if the file can be read doesn't help
          return;
        }

        try {

          // make a new, empty document
          Document doc = new Document();

          // Add the path of the file as a field named "path".  Use a
          // field that is indexed (i.e. searchable), but don't tokenize 
          // the field into separate words and don't index term frequency
          // or positional information:
          //Field pathField = new StringField("path", file.getPath(), Field.Store.YES);
          //doc.add(pathField);

          // Add the last modified date of the file a field named "modified".
          // Use a LongField that is indexed (i.e. efficiently filterable with
          // NumericRangeFilter).  This indexes to milli-second resolution, which
          // is often too fine.  You could instead create a number based on
          // year/month/day/hour/minutes/seconds, down the resolution you require.
          // For example the long value 2011021714 would mean
          // February 17, 2011, 2-3 PM.
          //Field modified = new StringField("modified",new Date(file.lastModified()).toString(),Field.Store.YES);
          //doc.add(modified);

          // Add the contents of the file to a field named "contents".  Specify a Reader,
          // so that the text of the file is tokenized and indexed, but not stored.
          // Note that FileReader expects the file to be in UTF-8 encoding.
          // If that's not the case searching for special characters will fail.
          DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
          DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
          org.w3c.dom.Document document = dBuilder.parse(file);
          document.getDocumentElement().normalize();
          addTextField("title",doc,document);
          addTextField("subject",doc,document);
          /*Ahora indexamos las propias palabras de la descripcion*/
          addTextField("description",doc,document);
          addTextField("creator",doc,document);
          /*Quiza esto se podria mejorar cambiando el tipo de Field a uno mas eficiente*/
          addIntField("date",doc,document);
          /*Ahora se deberia indexar con el analaizador custom el campo date2
          NodeList nList = document.getElementsByTagName("dc:description");
          String name = null;
          for (int temp = 0; temp < nList.getLength(); temp++) {
              Node nNode = nList.item(temp);
              if(nNode.hasChildNodes()){
            	  name = nNode.getFirstChild().getNodeValue();
            	  /*Al usar la etiqueta date2 utiliza el analizador customizado definido/
                  doc.add(new TextField("date2", new BufferedReader(new StringReader(name))));
              }
          }*/

          if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
            // New index, so we just add the document (no old document can be there):
            System.out.println("adding " + file);
            writer.addDocument(doc);
          } else {
            // Existing index (an old copy of this document may have been indexed) so 
            // we use updateDocument instead to replace the old one matching the exact 
            // path, if present:
            System.out.println("updating " + file);
            writer.updateDocument(new Term("path", file.getPath()), doc);
          }
          
        } finally {
          fis.close();
        }
      }
    }
  }
  
  @SuppressWarnings("unused")
private static void addStringField(String etiqueta,Document doc,org.w3c.dom.Document document){
	  NodeList nList = document.getElementsByTagName("dc:"+etiqueta);
      String name = null;
      for (int temp = 0; temp < nList.getLength(); temp++) {
          Node nNode = nList.item(temp);
          if(nNode.hasChildNodes()){
        	  name = nNode.getFirstChild().getNodeValue();
              doc.add(new StringField(etiqueta, name, Field.Store.YES));
          }
      }
  }
  private static void addTextField(String etiqueta,Document doc,org.w3c.dom.Document document){
	  NodeList nList = document.getElementsByTagName("dc:"+etiqueta);
      String name = null;
      for (int temp = 0; temp < nList.getLength(); temp++) {
          Node nNode = nList.item(temp);
          if(nNode.hasChildNodes()){
        	  name = nNode.getFirstChild().getNodeValue();
              doc.add(new TextField(etiqueta, new BufferedReader(new StringReader(name))));
          }
      }
  }
  private static void addIntField(String etiqueta,Document doc,org.w3c.dom.Document document){
	  NodeList nList = document.getElementsByTagName("dc:"+etiqueta);
      String name = null;
      for (int temp = 0; temp < nList.getLength(); temp++) {
          Node nNode = nList.item(temp);
          if(nNode.hasChildNodes()){
        	  name = nNode.getFirstChild().getNodeValue();
              doc.add(new IntPoint(etiqueta, Integer.parseInt(name)));
          }
      }
  }
}