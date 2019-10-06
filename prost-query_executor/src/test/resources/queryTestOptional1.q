SELECT ?title ?genre
WHERE
{
	?book <http://example.org/title> ?title .
	OPTIONAL {?book <http://example.org/genre> ?genre .}
}