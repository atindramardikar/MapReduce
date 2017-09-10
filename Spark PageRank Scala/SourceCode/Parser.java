//java function for parser
package mapredpagerank;

import java.io.IOException;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import java.io.IOException;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

public class Parser{

ArrayList<String> outlinks = new ArrayList<String>();
        int jj = 0;
        int count = 0;
        private static Pattern namePattern;
        private static Pattern linkPattern;
        
        static {
            // Keep only html pages not containing tilde (~).
            namePattern = Pattern.compile("^([^~]+)$");
            // Keep only html filenames ending relative paths and not containing tilde (~).
            linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
        }
    public String pageRank(String line) throws Exception{
        
        try {
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            SAXParser saxParser = spf.newSAXParser();
            XMLReader xmlReader = saxParser.getXMLReader();
            // Parser fills this list with linked page names.
            List<String> linkPageNames = new LinkedList<>();
            xmlReader.setContentHandler(new WikiParser(linkPageNames));
            //String line = lines.toString();
            int delimLoc = line.indexOf(':');
            String pageName = line.substring(0, delimLoc);
            pageName=pageName.replaceAll(",","");
            String html = line.substring(delimLoc + 1);
            html = html.replaceAll("&", "&amp;"); //replacing "&"
            boolean flag = true;
            Matcher matcher = namePattern.matcher(pageName);
            if (!matcher.find()) {
                // Skip this html file, name contains (~).
                //continue;
                flag = false;
            }
            // Parse page and fill list of linked pages.
            linkPageNames.clear();
            try {
                xmlReader.parse(new InputSource(new StringReader(html)));
            } catch (Exception e) {
                // Discard ill-formatted pages.
                //continue;\
                flag = false;
            }
            //System.out.println(pageName+"-"+linkPageNames);
            
            StringBuilder sb = new StringBuilder();
            String finalstr = "";
            // Occasionally print the page and its links.
            for (String str : linkPageNames) {
                str=str.replaceAll(",","");
                finalstr += str + ";";
            }
            if(!finalstr.equals(""))
                finalstr = finalstr.substring(0, finalstr.length()-1);
            
            //System.out.println(finalstr);
            
            if (flag) {
                return pageName + ":" + finalstr; //return the final string
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }




    private static class WikiParser extends DefaultHandler {

        /**
         * List of linked pages; filled by parser.
         */
        private List<String> linkPageNames;
        /**
         * Nesting depth inside bodyContent div element.
         */
        private int count = 0;

        public WikiParser(List<String> linkPageNames) {
            super();
            this.linkPageNames = linkPageNames;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            super.startElement(uri, localName, qName, attributes);
            if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) {
                // Beginning of bodyContent div element.
                count = 1;
            } else if (count > 0 && "a".equalsIgnoreCase(qName)) {
                // Anchor tag inside bodyContent div element.
                count++;
                String link = attributes.getValue("href");
                if (link == null) {
                    return;
                }
                try {
                    // Decode escaped characters in URL.
                    link = URLDecoder.decode(link, "UTF-8");
                } catch (Exception e) {
                    // Wiki-weirdness; use link as is.
                }
                // Keep only html filenames ending relative paths and not containing tilde (~).
                Matcher matcher = linkPattern.matcher(link);
                if (matcher.find()) {
                    linkPageNames.add(matcher.group(1));
                }
            } else if (count > 0) {
                // Other element inside bodyContent div.
                count++;
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            super.endElement(uri, localName, qName);
            if (count > 0) {
                // End of element inside bodyContent div.
                count--;
            }
        }
    }

}
