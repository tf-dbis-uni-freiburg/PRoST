SELECT ?title
WHERE
{
	?book <http://example.org/title> ?title.
}
ORDER BY ?title
LIMIT 2