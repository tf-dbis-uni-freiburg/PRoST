SELECT ?title
WHERE
{
	?book <http://example.org/title> ?title.
}
GROUP BY ?title