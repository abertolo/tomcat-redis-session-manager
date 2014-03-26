package blackboard.catalina.session;

import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;

import javax.servlet.ServletException;
import java.io.IOException;

import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import java.util.regex.Pattern;

public class RedisSessionHandlerValve extends ValveBase {
	private final Log log = LogFactory.getLog(RedisSessionManager.class);

	protected String uriExclude = "/ping/.*";

	private Pattern uriExcludePattern;

	@Override
	public void invoke(Request request, Response response) throws IOException, ServletException {
		RedisSessionManager manager = (RedisSessionManager) getContainer().getManager();
		String fullUri = getRequestFullUri(request);
		boolean exclude = getUriExcludePattern().matcher(fullUri).matches();
		try {
			if(!exclude)
				manager.beforeRequest(request, fullUri);
			
			getNext().invoke(request, response);

		} finally {
			if(!exclude)
				manager.afterRequest(request);
		}
	}

	public String getRequestFullUri(Request request){
		return request.getRequestURI() + (request.getQueryString() != null ? "?" + request.getQueryString() : "");
	}

	public String getUriExclude() {
		return uriExclude;
	}

	public void setUriExclude(String ue) {
		this.uriExclude = ue;
	}

	private Pattern getUriExcludePattern(){
		if(uriExcludePattern == null){
			uriExcludePattern = Pattern.compile(getUriExclude());
			log.info("getUriExcludePattern compile regex: " + getUriExclude());
		}
		return uriExcludePattern;
	}

}