SELECT ?name
WHERE { ?person <http://example.org/name> ?name .
         OPTIONAL { ?person <http://example.org/mail> ?mail } .
         FILTER ( !bound(?mail) ) }