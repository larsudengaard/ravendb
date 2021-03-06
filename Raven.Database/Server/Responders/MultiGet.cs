using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Security.Principal;
using System.Web;
using NLog;
using Raven.Abstractions.Data;
using Raven.Http;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

namespace Raven.Database.Server.Responders
{
	public class MultiGet : RequestResponder
	{
		public override string UrlPattern
		{
			get { return @"^/multi_get/?$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "POST" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var results = new List<GetResponse>();
			var requests = context.ReadJsonObject<GetRequest[]>();
			Database.TransactionalStorage.Batch(accessor => // ensure all queries are transactionally the same
			{
				foreach (var req in requests)
				{
					var ctx = new MultiGetHttpContext(Settings, context, req);
					server.HandleActualRequest(ctx);
					results.Add(ctx.Complete());
				}
			});
			context.WriteJson(results);
		}

		public class MultiGetHttpContext : IHttpContext
		{
			private readonly IRavenHttpConfiguration configuration;
			private readonly IHttpContext realContext;
			private readonly GetResponse getResponse;

			public MultiGetHttpContext(IRavenHttpConfiguration configuration, IHttpContext realContext, GetRequest req)
			{
				this.configuration = configuration;
				this.realContext = realContext;
				getResponse = new GetResponse();
				Request = new MultiGetHttpRequest(req, realContext.Request);
				Response = new MultiGetHttpResponse(getResponse, realContext.Response);
			}

			public GetResponse Complete()
			{
				if(getResponse.Result!=null)
					return getResponse;

				Response.OutputStream.Position = 0;
				getResponse.Result = new StreamReader(Response.OutputStream).ReadToEnd();
				getResponse.Status = Response.StatusCode;
				return getResponse;
			}

			public IRavenHttpConfiguration Configuration
			{
				get { return configuration; }
			}

			public IHttpRequest Request { get; set; }

			public IHttpResponse Response { get; set; }

			public IPrincipal User
			{
				get { return realContext.User; }
			}


			public void FinalizeResonse()
			{
				// nothing here
			}

			public void SetResponseFilter(Func<Stream, Stream> responseFilter)
			{
				// nothing here
			}

			public void OutputSavedLogItems(Logger logger)
			{
				realContext.OutputSavedLogItems(logger);
			}

			public void Log(Action<Logger> loggingAction)
			{
				realContext.Log(loggingAction);
			}
		}

		public class MultiGetHttpRequest : IHttpRequest
		{
			private readonly GetRequest req;

			public MultiGetHttpRequest(GetRequest req, IHttpRequest realRequest)
			{
				this.req = req;
				QueryString = HttpUtility.ParseQueryString(req.Query ?? "");
				Url = new UriBuilder(realRequest.Url)
				{
					Query = req.Query,
					Path = req.Url
				}.Uri;
				RawUrl = Url.ToString();
				Headers = new NameValueCollection();
				foreach (var header in req.Headers)
				{
					Headers.Add(header.Key, header.Value);
				}
			}

			public NameValueCollection Headers { get; set; }

			public Stream InputStream
			{
				get { return Stream.Null; }
			}

			public NameValueCollection QueryString { get; set; }

			public string HttpMethod
			{
				get { return "GET"; }
			}

			public Uri Url
			{
				get;
				set;
			}

			public string RawUrl
			{
				get;
				set;
			}
		}


		public class MultiGetHttpResponse : IHttpResponse
		{
			private readonly GetResponse getResponse;

			public MultiGetHttpResponse(GetResponse getResponse, IHttpResponse response)
			{
				this.getResponse = getResponse;
				RedirectionPrefix = response.RedirectionPrefix;
				OutputStream = new MemoryStream();

			}

			public string RedirectionPrefix
			{
				get;
				set;
			}

			public void AddHeader(string name, string value)
			{
				getResponse.Headers[name] = value;
			}

			public Stream OutputStream { get; set; }

			public long ContentLength64
			{
				get;
				set;
			}

			public int StatusCode
			{
				get;
				set;
			}

			public string StatusDescription
			{
				get;
				set;
			}

			public string ContentType
			{
				get;
				set;
			}

			public void Redirect(string url)
			{
				getResponse.Status = 301;
				getResponse.Headers["Location"] = url;
			}

			public void Close()
			{
			}

			public void SetPublicCachability()
			{
				getResponse.Headers["Cache-Control"] = "Public";
			}

			public void WriteFile(string path)
			{
				using (var file = File.OpenRead(path))
				{
					file.CopyTo(OutputStream);
				}
			}
		}
	}

}