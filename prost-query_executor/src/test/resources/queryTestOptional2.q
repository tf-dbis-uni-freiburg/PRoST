SELECT ?title ?genre ?author
WHERE
{
	?book <http://example.org/title> ?title.
	OPTIONAL {?book <http://example.org/genre> ?genre}.
	OPTIONAL {?book <http://example.org/author> ?author}
}