package FirstRound;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstRoundMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	private Set<String> titleFilter;
	private Set<String> endFilter;
	private String date;
	private Text titleDate = new Text();
	private LongWritable views = new LongWritable();
	
	@Override
	protected void setup(Context context) 
			throws IOException, InterruptedException
    {
		buildTitleFilter();
		buildEndFilter();
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String  fileName = fileSplit.getPath().getName();
		date = fileName.substring(fileName.indexOf("-") + 1, fileName.lastIndexOf("-"));
	}
	@Override
	protected void map(LongWritable offset, Text value, Context context) 
            throws IOException, InterruptedException {
		
		String[]tokens = value.toString().split(" ");
		if(tokens.length != 4) return;
		
		String project = tokens[0].trim().toLowerCase();
		String pageName = tokens[1].trim();
		int pageViews = Integer.parseInt(tokens[2].trim().toLowerCase());
		if(!project.equalsIgnoreCase("en")) return;
		StringBuilder sb = new StringBuilder();
		if(isGoodTitle(pageName)) {
			pageName = pageName.replaceAll("%22", "_");
			sb.append(pageName);
			sb.append("}");
			sb.append(date);
			titleDate.set(sb.toString());
			views.set(pageViews);
			context.write(titleDate, views);
		}	
    }
	
	private boolean isGoodTitle(String title) {
		if(title.length() > 0 && title.charAt(0) >= 'a' && title.charAt(0) <= 'z') {
			return false;
		}
		String tmp = title.toLowerCase();
		if(titleFilter.contains(tmp) || endFilter.contains(tmp)) {
			return false;
		}
		for(String filter : titleFilter) {
			if(tmp.startsWith(filter)) {
				return false;
			}
		}
		for(String filter : endFilter) {
			if(tmp.endsWith(filter)) {
				return false;
			}
		}
		return true;
	}
	
	private void buildEndFilter() {
		endFilter = new HashSet<String>();
		endFilter.add("jpg");
		endFilter.add("gif");
		endFilter.add("png");
		endFilter.add("ico");
		endFilter.add("txt");
	}
	private void buildTitleFilter() {
		titleFilter = new HashSet<String>();
		titleFilter.add("404_error");
		titleFilter.add("main_page");
		titleFilter.add("hypertext_transfer_protocol");
		titleFilter.add("favicon.ico");
		titleFilter.add("search");
		
		
		titleFilter.add("media");
		titleFilter.add("special");
		titleFilter.add("talk");
		titleFilter.add("user");
		titleFilter.add("user_talk");
		titleFilter.add("project");
		
		titleFilter.add("project_talk");
		titleFilter.add("file");
		titleFilter.add("file_talk");
		
		titleFilter.add("mediaWiki");
		titleFilter.add("mediaWiki_talk");
		titleFilter.add("template");
		titleFilter.add("template_talk");
		
		titleFilter.add("help");
		titleFilter.add("help_talk");
		titleFilter.add("category");
		titleFilter.add("category_talk");
		
		titleFilter.add("portal");
		titleFilter.add("wikipedia");
		titleFilter.add("wikipedia_talk");
		
		
	}


    
}