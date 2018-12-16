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

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.Normalizer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.WhitespaceTokenizer;
import opennlp.tools.util.Span;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.core.StopFilterFactory;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.es.SpanishLightStemFilterFactory;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilterFactory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizerFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import static org.apache.lucene.analysis.es.SpanishAnalyzer.getDefaultStopSet;

/** Simple command-line based search demo. */
public class SearchFiles {

	private SearchFiles() {
	}

	/** Simple command-line based search demo. */
	public static void main(String[] args) throws Exception {
		String usage = "Usage:\tjava org.apache.lucene.demo.SearchFiles [-index dir] [-field f] [-repeat n] [-queries file] [-query string] [-raw] [-paging hitsPerPage]\n\nSee http://lucene.apache.org/core/4_1_0/demo/ for details.";
		if (args.length > 0 && ("-h".equals(args[0]) || "-help".equals(args[0]))) {
			System.out.println(usage);
			System.exit(0);
		}
		String index = "index";
		String field = "contents";
		PrintStream out = null;
		List<String> need_id=new ArrayList<String>();
		String queries = "";
		String infoNeeds = null;
		int id=0;
		int repeat = 0;
		boolean raw = false;
		String queryString = null;
		int hitsPerPage = 100000000;

		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				index = args[i + 1];
				i++;
			} else if ("-field".equals(args[i])) {
				field = args[i + 1];
				i++;
			} else if ("-output".equals(args[i])) {
				out = new PrintStream(new File(args[i + 1]));
				System.setOut(out);
				i++;
			} else if ("-infoNeeds".equals(args[i])) {
				infoNeeds = args[i + 1];
				i++;
			} else if ("-query".equals(args[i])) {
				queryString = args[i + 1];
				i++;
			} else if ("-repeat".equals(args[i])) {
				repeat = Integer.parseInt(args[i + 1]);
				i++;
			} else if ("-raw".equals(args[i])) {
				raw = true;
			} else if ("-paging".equals(args[i])) {
				hitsPerPage = Integer.parseInt(args[i + 1]);
				if (hitsPerPage <= 0) {
					System.err.println("There must be at least 1 hit per page.");
					System.exit(1);
				}
				i++;
			}
		}
		Path Fsw = Paths.get("stopwords.txt");
		Charset charset = Charset.forName("UTF-8");
		Set<String> setWords = new HashSet<String>(Files.readAllLines(Fsw, charset));

		CharArraySet stopWords = getDefaultStopSet();
		stopWords.addAll(setWords);
		Query qcreator;
		List<Query> qdate = new ArrayList<Query>();
		IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(index)));
		IndexSearcher searcher = new IndexSearcher(reader);
		Analyzer analyzer = CustomAnalyzer.builder(Paths.get(index)).withTokenizer(StandardTokenizerFactory.class)
				.addTokenFilter(LowerCaseFilterFactory.class).addTokenFilter(ASCIIFoldingFilterFactory.class)
				.addTokenFilter(StopFilterFactory.class, "ignoreCase", "false", "words", "..\\stopwords.txt", "format",
						"wordset")
				.addTokenFilter(SpanishLightStemFilterFactory.class)

				.build();
		InputStream inputStreamNameFinder = new FileInputStream("es-ner-person.bin");
		TokenNameFinderModel personModel = new TokenNameFinderModel(inputStreamNameFinder);
		NameFinderME nameFinder = new NameFinderME(personModel);
		WhitespaceTokenizer simpleTokenizer = WhitespaceTokenizer.INSTANCE;
		BufferedReader in = null;
		if (infoNeeds != null) {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			org.w3c.dom.Document document = dBuilder.parse(infoNeeds);
			document.getDocumentElement().normalize();
			NodeList nList = document.getElementsByTagName("text");
			for (int temp = 0; temp < nList.getLength(); temp++) {
				Node nNode = nList.item(temp);
				if (nNode.hasChildNodes()) {
					queries += nNode.getFirstChild().getNodeValue() + "\n";
				}
			}
			nList = document.getElementsByTagName("identifier");
			for (int temp = 0; temp < nList.getLength(); temp++) {
				Node nNode = nList.item(temp);
				if (nNode.hasChildNodes()) {
					need_id.add(nNode.getFirstChild().getNodeValue());
				}
			}
			in = new BufferedReader(new StringReader(queries));
		} else {
			in = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
		}

		while (true) {
			if (infoNeeds == null && queryString == null) { // prompt the user
				System.out.println("Enter query: ");
			}

			String line = queryString != null ? queryString : in.readLine();

			if (line == null || line.length() == -1) {
				break;
			}

			line = line.trim();

			if (line.length() == 0) {
				break;
			}
			BooleanQuery.Builder builderConsulta = new BooleanQuery.Builder();
			String line2 = new String(line);
			line2 = Normalizer.normalize(line2, Normalizer.Form.NFD);
			line2 = line2.replaceAll("[^\\p{ASCII}]", "");
			line2 = line2.replace('.', ' ');
			// Instantiating the TokenizerME class
			String tokens[] = simpleTokenizer.tokenize(line2);
			// Tokenizing the given raw text
			Span nameSpans[] = nameFinder.find(tokens);
			Pattern pattern = Pattern.compile("a partir de(|l) \\d\\d\\d\\d", Pattern.CASE_INSENSITIVE);
			Matcher matcher = pattern.matcher(line2);
			// check all occurance
			while (matcher.find()) {
				Integer from = Integer
						.parseInt(matcher.group().substring(matcher.group().length() - 4, matcher.group().length()));
				qdate.add(IntPoint.newRangeQuery("date", from, 2018));
			}
			pattern = Pattern.compile("entre \\d\\d\\d\\d y \\d\\d\\d\\d", Pattern.CASE_INSENSITIVE);
			matcher = pattern.matcher(line2);
			while (matcher.find()) {
				Integer until = Integer
						.parseInt(matcher.group().substring(matcher.group().length() - 4, matcher.group().length()));
				Integer from = Integer.parseInt(matcher.group().substring(6, 10));
				qdate.add(IntPoint.newRangeQuery("date", from, until));
			}
			pattern = Pattern.compile("\\d\\d\\d\\d(-|/|:)\\d\\d\\d\\d", Pattern.CASE_INSENSITIVE);
			matcher = pattern.matcher(line2);
			while (matcher.find()) {
				Integer until = Integer
						.parseInt(matcher.group().substring(matcher.group().length() - 4, matcher.group().length()));
				Integer from = Integer.parseInt(matcher.group().substring(0, 4));
				qdate.add(IntPoint.newRangeQuery("date", from, until));
			}
			pattern = Pattern.compile("ultimos \\d+ anos", Pattern.CASE_INSENSITIVE);
			matcher = pattern.matcher(line2);
			while (matcher.find()) {
				Pattern pattern2 = Pattern.compile("\\d+", Pattern.CASE_INSENSITIVE);
				Matcher matcher2 = pattern2.matcher(matcher.group());
				matcher2.find();
				Integer from = 2018 - Integer.parseInt(matcher2.group());
				qdate.add(IntPoint.newRangeQuery("date", from, 2018));
			}
			pattern = Pattern.compile("ultimo ano", Pattern.CASE_INSENSITIVE);
			matcher = pattern.matcher(line2);
			while (matcher.find()) {
				qdate.add(IntPoint.newRangeQuery("date", 2018, 2018));
			}
			Term term = new Term(line);

			FuzzyQuery errores = new FuzzyQuery(term);

			QueryParser parsert = new QueryParser("title", analyzer);
			Query titulo = parsert.parse(line);

			QueryParser parsers = new QueryParser("subject", analyzer);
			Query subject = parsers.parse(line);
			QueryParser parserd = new QueryParser("description", analyzer);
			Query description = parserd.parse(line);

			builderConsulta.add(new BoostQuery(titulo, 2), BooleanClause.Occur.SHOULD)
					.add(new BoostQuery(subject, 2.5f), BooleanClause.Occur.SHOULD)
					.add(new BoostQuery(description, 0.5f), BooleanClause.Occur.SHOULD)
					.add(new BoostQuery(errores, 0.4f), BooleanClause.Occur.SHOULD);
			for (Query queryaux : qdate)
				builderConsulta.add(new BoostQuery(queryaux, 4), BooleanClause.Occur.SHOULD);
			if (nameSpans.length > 0) {
				String names[] = Span.spansToStrings(nameSpans, tokens);
				String nombres = "";
				for (String i : names)
					nombres += i + " ";
				QueryParser creator = new QueryParser("creator", new StandardAnalyzer());
				qcreator = creator.parse(nombres);
				builderConsulta.add(new BoostQuery(qcreator, 4.5f), BooleanClause.Occur.SHOULD);
			}
			Query query = builderConsulta.build();

			if (repeat > 0) { // repeat & time as benchmark
				Date start = new Date();
				for (int i = 0; i < repeat; i++) {
					searcher.search(query, 100);
				}
				Date end = new Date();
			}

			doPagingSearch(queryString, searcher, query, hitsPerPage, raw, queries == null && queryString == null, need_id.get(id));
			id++;
			if (queryString != null) {
				break;
			}
		}
		reader.close();
	}

	/**
	 * This demonstrates a typical paging search scenario, where the search
	 * engine presents pages of size n to the user. The user can then go to the
	 * next page if interested in the next hits.
	 *
	 * When the query is executed for the first time, then only enough results
	 * are collected to fill 5 result pages. If the user wants to page beyond
	 * this limit, then the query is executed another time and all hits are
	 * collected.
	 *
	 */
	public static void doPagingSearch(String line, IndexSearcher searcher, Query query, int hitsPerPage,
			boolean raw, boolean interactive, String need_id) throws IOException {

		// Collect enough docs to show 5 pages
		TopDocs results = searcher.search(query, 5 * hitsPerPage);
		ScoreDoc[] hits = results.scoreDocs;

		int numTotalHits = (int) results.totalHits;
		int start = 0;
		int end = Math.min(numTotalHits, hitsPerPage);

		while (true) {
			if (end > hits.length) {
				System.out.println("Only results 1 - " + hits.length + " of " + numTotalHits
						+ " total matching documents collected.");
				System.out.println("Collect more (y/n) ?");
				if (line.length() == 0 || line.charAt(0) == 'n') {
					break;
				}

				hits = searcher.search(query, numTotalHits).scoreDocs;
			}

			// end = Math.min(hits.length, start + hitsPerPage);
			end = hits.length;

			for (int i = start; i < end; i++) {
				if (raw) { // output raw format
					System.out.println("doc=" + hits[i].doc + " score=" + hits[i].score);
					continue;
				}

				Document doc = searcher.doc(hits[i].doc);
				String path = doc.get("path");
				if (path != null) {
					System.out.println(need_id + "\t" + path);
				} else {
					System.out.println((i + 1) + ". " + "No path for this document");
				}

			}

			if (!interactive || end == 0) {
				break;
			}

			if (numTotalHits >= end) {
				boolean quit = false;
				while (true) {
					System.out.print("Press ");
					if (start - hitsPerPage >= 0) {
						System.out.print("(p)revious page, ");
					}
					if (start + hitsPerPage < numTotalHits) {
						System.out.print("(n)ext page, ");
					}
					System.out.println("(q)uit or enter number to jump to a page.");

					if (line.length() == 0 || line.charAt(0) == 'q') {
						quit = true;
						break;
					}
					if (line.charAt(0) == 'p') {
						start = Math.max(0, start - hitsPerPage);
						break;
					} else if (line.charAt(0) == 'n') {
						if (start + hitsPerPage < numTotalHits) {
							start += hitsPerPage;
						}
						break;
					} else {
						int page = Integer.parseInt(line);
						if ((page - 1) * hitsPerPage < numTotalHits) {
							start = (page - 1) * hitsPerPage;
							break;
						} else {
							System.out.println("No such page");
						}
					}
				}
				if (quit)
					break;
				end = Math.min(numTotalHits, start + hitsPerPage);
			}
		}
	}
}