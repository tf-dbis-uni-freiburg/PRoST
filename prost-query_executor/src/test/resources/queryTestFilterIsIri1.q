SELECT ?title ?mail
WHERE
{ ?book <http://example.org/title> ?title.
  ?book <http://example.org/mail> ?mail. FILTER isIRI(?mail) }