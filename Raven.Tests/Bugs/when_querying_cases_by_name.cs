using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using Raven.Client.Embedded;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class when_querying_cases_by_name_using_danish_collation : IDisposable
	{
		private readonly EmbeddableDocumentStore store;

        public when_querying_cases_by_name_using_danish_collation()
        {
            var culture = new CultureInfo("da");
            Thread.CurrentThread.CurrentCulture = culture;
            Thread.CurrentThread.CurrentUICulture = culture;

            store = new EmbeddableDocumentStore
            {
                RunInMemory = true
            };
            store.Initialize();

            using (var session = store.OpenSession())
            {
                session.Store(new Case { Name = "bcda" });
                session.Store(new Case { Name = "dacb" });
                session.Store(new Case { Name = "daab" });
                session.Store(new Case { Name = "dacb" });
                session.Store(new Case { Name = "aacb" });
                session.Store(new Case { Name = "aaac" });
                session.Store(new Case { Name = "bcbb" });
                session.Store(new Case { Name = "acba" });
                session.Store(new Case { Name = "aaaa" });
                session.Store(new Case { Name = "dada" });
                session.SaveChanges();
            }
        }

        [Fact]
        public void can_query_names_starting_with_da()
        {
            using (var session = store.OpenSession())
            {
                var cases = session.Query<Case>()
                    .Customize(x => x.WaitForNonStaleResultsAsOfLastWrite())
                    .Where(x => x.Name.StartsWith("da")).ToList();
                
                Assert.Equal(4, cases.Count);
				Assert.Equal(new[] { "dacb", "daab", "dacb", "dada" }, cases.Select(x => x.Name).ToArray());
			}
        }

        [Fact]
        public void can_query_names_starting_with_bc()
        {
            using (var session = store.OpenSession())
            {
                var cases = session.Query<Case>()
                    .Customize(x => x.WaitForNonStaleResultsAsOfLastWrite())
                    .Where(x => x.Name.StartsWith("bc")).ToList();

				Assert.Equal(2, cases.Count);
				Assert.Equal(new[] { "bcda", "bcbb" }, cases.Select(x => x.Name).ToArray());
            }
        }

		public void Dispose()
		{
			store.Dispose();
			
		}

		public class Case
		{
			public string Name { get; set; }
		}
	}
}