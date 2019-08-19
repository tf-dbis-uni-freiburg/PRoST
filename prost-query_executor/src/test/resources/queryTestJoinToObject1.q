SELECT ?title ?publisher ?name
WHERE
{
	?book <http://example.org/title> ?title.
	?book <http://example.org/publishedBy> ?publisher.
	?publisher <http://example.org/name> ?name.
}