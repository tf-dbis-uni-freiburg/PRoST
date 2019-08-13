SELECT ?title ?author ?name
WHERE
{
	?book <http://example.org/title> ?title.
	OPTIONAL {?book <http://example.org/writtenBy> ?author.
			OPTIONAL {?author <http://example.org/name> ?name}}
}