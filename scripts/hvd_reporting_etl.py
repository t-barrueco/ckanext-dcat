#!/usr/bin/env python3
"""HVD Reporting ETL.

Builds a local RDF graph with the HVD (High Value Datasets) metadata published
on data.europa.eu plus the controlled vocabularies it references, runs the
LP-ETL reporting queries against it, and writes the CSV files that feed the
HVD Reporting application (HVD_REPORTING_TOOL_V2).

The script is a straight port of the "HVD Reporting - local RDF graph +
reporting queries" notebook and works in two phases:

Phase A (network) - fetch data into a local rdflib Dataset:
  * the Countries, EU Licences and HVD Categories vocabularies, each into its
    own named graph (same graph IRIs LP-ETL uses in Virtuoso);
  * the HVD dataset metadata, via several CONSTRUCT queries against
    https://data.europa.eu/sparql (global first, per-country fallback when the
    endpoint times out).
  The full Dataset is persisted as N-Quads so later runs can skip the fetch.

Phase B (local) - run the 11 reporting queries (Q5, Q6, Q7, Q8, Q-MS1..4 and
variants) against the local graph and save one CSV per query, plus the
hvd-tagged.ttl / hvd-valid.ttl dumps, and optionally zip everything.

Phase C (local) - compute the per-catalogue compliance checks consumed by the
HVD reporting dashboard and write one compliance_<catalogue_id>.json per
catalogue into <output-dir>/dashboard/data/, mirroring the app's layout, plus
a catalogues.json manifest (catalogue id/IRI, country, dataset count). Copy
the dashboard/data folder into the app checkout to feed it real data.

Usage:
    pip install rdflib
    python hvd_reporting_etl.py                       # full run
    python hvd_reporting_etl.py --refresh             # ignore the cache
    python hvd_reporting_etl.py --output-dir out --zip report.zip
    python hvd_reporting_etl.py --app-data-dir ../HVD_REPORTING_TOOL_V2/dashboard/data

Outputs (in --output-dir, default ./hvdReport):
    Q8 - Current numbers of HVDs in countries - without quality checks.csv
    Q7 - Current numbers of HVDs in countries with quality checks.csv
    Q5-nz.csv
    Q5 - HVD granular category-dataset coverage per country.csv
    Q6-nz.csv
    Q6 - HVD top level category coverage per country.csv
    Q-MS1.csv
    Q-MS2.csv
    Q-MS2-no-filter.csv
    Q-MS3.csv
    Q-MS4.csv
    hvd-tagged.ttl
    hvd-valid.ttl
    dashboard/data/compliance_<catalogue_id>.json   (one per catalogue)
    dashboard/data/catalogues.json
"""

import argparse
import csv
import datetime
import io
import json
import logging
import os
import sys
import time
import zipfile
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from rdflib import Dataset, Graph, URIRef

log = logging.getLogger("hvd-etl")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_ENDPOINT = "https://data.europa.eu/sparql"
DEFAULT_OUTPUT_DIR = "hvdReport"
DEFAULT_CACHE = "hvd_dataset.nq"

# Named graph IRIs (same as LP-ETL)
GRAPH_COUNTRIES = URIRef("http://publications.europa.eu/resource/authority/country")
GRAPH_LICENCES = URIRef("http://publications.europa.eu/resource/authority/licence")
GRAPH_HVD_CATS = URIRef("http://data.europa.eu/bna/asd487ae75")
GRAPH_HVD_DATASETS = URIRef("http://data.europa.eu/hvd/datasets")

HTTP_HEADERS = {
    "Accept": "application/rdf+xml, text/turtle, */*",
    "User-Agent": "HVD-ETL/1.0 (Python rdflib)",
}

VOCABULARIES = [
    {
        "name": "EU Licences",
        "url": "http://publications.europa.eu/resource/distribution/licence/rdf/skos_core/licences-skos.rdf",
        "graph_iri": GRAPH_LICENCES,
        "format": "xml",
    },
    {
        "name": "Countries",
        "url": "http://publications.europa.eu/resource/distribution/country/rdf/skos_core/countries-skos.rdf",
        "graph_iri": GRAPH_COUNTRIES,
        "format": "xml",
    },
    {
        "name": "HVD Categories",
        "url": "http://publications.europa.eu/resource/distribution/high-value-dataset-category/rdf/skos_core/high-value-dataset-category.rdf",
        "graph_iri": GRAPH_HVD_CATS,
        "format": "xml",
    },
]

# ---------------------------------------------------------------------------
# Phase A - CONSTRUCT queries that populate the HVD datasets named graph
# ---------------------------------------------------------------------------

PREFIX_BLOCK = """
PREFIX vcard:   <http://www.w3.org/2006/vcard/ns#>
PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:     <http://www.w3.org/2002/07/owl#>
PREFIX skos:    <http://www.w3.org/2004/02/skos/core#>
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX dcat:    <http://www.w3.org/ns/dcat#>
PREFIX dcatap:  <http://data.europa.eu/r5r/>
PREFIX hvd-ir:  <http://data.europa.eu/eli/reg_impl/2023/138/oj>
"""

# __SPATIAL__ gets replaced with a fixed country IRI for the per-country fallback
SPATIAL_GLOBAL = "?catalog a dcat:Catalog ; dcterms:spatial ?country ; dcat:dataset ?dataset ."

# C1: catalogs, records, dataset core metadata
CONSTRUCT_CORE = PREFIX_BLOCK + """
CONSTRUCT {
  ?catalog a dcat:Catalog ;
           dcterms:spatial ?country ;
           dcat:dataset ?dataset .
  ?record a dcat:CatalogRecord ;
          foaf:primaryTopic ?dataset ;
          dcterms:identifier ?identifier .
  ?dataset a dcat:Dataset ;
           dcatap:applicableLegislation hvd-ir: ;
           dcatap:hvdCategory ?hvdCategory ;
           dcterms:title ?title ;
           dcterms:description ?description .
}
WHERE {
  __SPATIAL__
  ?dataset a dcat:Dataset ;
           dcatap:applicableLegislation hvd-ir: .
  OPTIONAL {
    ?record a dcat:CatalogRecord ;
            foaf:primaryTopic ?dataset ;
            dcterms:identifier ?identifier .
  }
  OPTIONAL { ?dataset dcatap:hvdCategory ?hvdCategory }
  OPTIONAL { ?dataset dcterms:title ?title .
             FILTER (langMatches(LANG(?title), "en")) }
  OPTIONAL { ?dataset dcterms:description ?description .
             FILTER (langMatches(LANG(?description), "en")) }
}
"""

# C2: distributions
CONSTRUCT_DISTRIBUTIONS = PREFIX_BLOCK + """
CONSTRUCT {
  ?dataset dcat:distribution ?distribution .
  ?distribution a dcat:Distribution ;
                dcatap:applicableLegislation ?legislation ;
                dcterms:identifier ?distIdentifier ;
                dcat:accessURL ?accessURL ;
                dcterms:license ?license ;
                dcat:downloadURL ?downloadURL ;
                dcat:accessService ?accessService .
}
WHERE {
  __SPATIAL__
  ?dataset a dcat:Dataset ;
           dcatap:applicableLegislation hvd-ir: ;
           dcat:distribution ?distribution .
  ?distribution a dcat:Distribution .
  OPTIONAL { ?distribution dcatap:applicableLegislation ?legislation }
  OPTIONAL { ?distribution dcterms:identifier ?distIdentifier }
  OPTIONAL { ?distribution dcat:accessURL ?accessURL }
  OPTIONAL { ?distribution dcterms:license ?license }
  OPTIONAL { ?distribution dcat:downloadURL ?downloadURL }
  OPTIONAL { ?distribution dcat:accessService ?accessService }
}
"""

# C3a: standalone data services (dcat:servesDataset)
CONSTRUCT_STANDALONE_SERVICES = PREFIX_BLOCK + """
CONSTRUCT {
  ?service a dcat:DataService ;
           dcat:servesDataset ?dataset ;
           dcatap:applicableLegislation ?legislation ;
           dcatap:hvdCategory ?svcCategory ;
           dcat:endpointURL ?endpointURL ;
           dcat:contactPoint ?contactPoint ;
           dcterms:license ?svcLicense ;
           foaf:page ?page .
  ?contactPoint vcard:hasURL ?cpURL ;
                vcard:hasEmail ?cpEmail .
}
WHERE {
  __SPATIAL__
  ?dataset a dcat:Dataset ;
           dcatap:applicableLegislation hvd-ir: .
  ?service dcat:servesDataset ?dataset ;
           a dcat:DataService .
  OPTIONAL { ?service dcatap:applicableLegislation ?legislation }
  OPTIONAL { ?service dcatap:hvdCategory ?svcCategory }
  OPTIONAL { ?service dcat:endpointURL ?endpointURL }
  OPTIONAL { ?service dcterms:license ?svcLicense }
  OPTIONAL { ?service foaf:page ?page }
  OPTIONAL {
    ?service dcat:contactPoint ?contactPoint .
    OPTIONAL { ?contactPoint vcard:hasURL ?cpURL }
    OPTIONAL { ?contactPoint vcard:hasEmail ?cpEmail }
  }
}
"""

# C3b: data services reached via distributions (dcat:accessService)
# (doesn't add dcat:servesDataset links that aren't in the source data)
CONSTRUCT_DISTRIBUTION_SERVICES = PREFIX_BLOCK + """
CONSTRUCT {
  ?service a dcat:DataService ;
           dcatap:applicableLegislation ?legislation ;
           dcatap:hvdCategory ?svcCategory ;
           dcat:endpointURL ?endpointURL ;
           dcat:contactPoint ?contactPoint ;
           dcterms:license ?svcLicense ;
           foaf:page ?page .
  ?contactPoint vcard:hasURL ?cpURL ;
                vcard:hasEmail ?cpEmail .
}
WHERE {
  __SPATIAL__
  ?dataset a dcat:Dataset ;
           dcatap:applicableLegislation hvd-ir: ;
           dcat:distribution/dcat:accessService ?service .
  ?service a dcat:DataService .
  OPTIONAL { ?service dcatap:applicableLegislation ?legislation }
  OPTIONAL { ?service dcatap:hvdCategory ?svcCategory }
  OPTIONAL { ?service dcat:endpointURL ?endpointURL }
  OPTIONAL { ?service dcterms:license ?svcLicense }
  OPTIONAL { ?service foaf:page ?page }
  OPTIONAL {
    ?service dcat:contactPoint ?contactPoint .
    OPTIONAL { ?contactPoint vcard:hasURL ?cpURL }
    OPTIONAL { ?contactPoint vcard:hasEmail ?cpEmail }
  }
}
"""

# C4: licence mappings to the EU licence vocabulary
CONSTRUCT_LICENSE_MAPPINGS = PREFIX_BLOCK + """
CONSTRUCT {
  ?license ?rel ?euLicense .
}
WHERE {
  __SPATIAL__
  ?dataset a dcat:Dataset ;
           dcatap:applicableLegislation hvd-ir: .
  {
    ?dataset dcat:distribution/dcterms:license ?license .
  }
  UNION
  {
    ?service dcat:servesDataset ?dataset ;
             dcterms:license ?license .
  }
  ?license ?rel ?euLicense .
  VALUES ?rel { skos:exactMatch skos:broadMatch owl:sameAs
                skos:narrowMatch skos:closeMatch rdfs:seeAlso }
}
"""

# C5/C6: markers for datasets with no title/description in ANY language.
# C1 only copies the English literals into the local graph, so "missing
# entirely" can only be checked remotely; these keep the result as one tiny
# marker triple per offending dataset.
MISSING_TITLE_FLAG = URIRef("urn:x-hvd-etl:missingTitle")
MISSING_DESCRIPTION_FLAG = URIRef("urn:x-hvd-etl:missingDescription")

CONSTRUCT_MISSING_TITLE = PREFIX_BLOCK + """
CONSTRUCT { ?dataset <urn:x-hvd-etl:missingTitle> true }
WHERE {
  __SPATIAL__
  ?dataset a dcat:Dataset ;
           dcatap:applicableLegislation hvd-ir: .
  FILTER NOT EXISTS { ?dataset dcterms:title ?anyTitle }
}
"""

CONSTRUCT_MISSING_DESCRIPTION = PREFIX_BLOCK + """
CONSTRUCT { ?dataset <urn:x-hvd-etl:missingDescription> true }
WHERE {
  __SPATIAL__
  ?dataset a dcat:Dataset ;
           dcatap:applicableLegislation hvd-ir: .
  FILTER NOT EXISTS { ?dataset dcterms:description ?anyDescription }
}
"""

CONSTRUCT_STEPS = [
    ("C1  core metadata", CONSTRUCT_CORE),
    ("C2  distributions", CONSTRUCT_DISTRIBUTIONS),
    ("C3a standalone services", CONSTRUCT_STANDALONE_SERVICES),
    ("C3b distribution services", CONSTRUCT_DISTRIBUTION_SERVICES),
    ("C4  licence mappings", CONSTRUCT_LICENSE_MAPPINGS),
    ("C5  missing-title markers", CONSTRUCT_MISSING_TITLE),
    ("C6  missing-description markers", CONSTRUCT_MISSING_DESCRIPTION),
]

SELECT_COUNTRIES = PREFIX_BLOCK + """
SELECT DISTINCT ?country
WHERE {
  ?catalog a dcat:Catalog ;
           dcterms:spatial ?country .
  FILTER (STRSTARTS(STR(?country),
    "http://publications.europa.eu/resource/authority/country/"))
}
"""

# ---------------------------------------------------------------------------
# Phase B - reporting queries (from the LP-ETL pipeline, unchanged)
# ---------------------------------------------------------------------------

PREFIXES = """
PREFIX vcard:   <http://www.w3.org/2006/vcard/ns#>
PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:     <http://www.w3.org/2002/07/owl#>
PREFIX skos:    <http://www.w3.org/2004/02/skos/core#>
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX dcat:    <http://www.w3.org/ns/dcat#>
PREFIX dcatap:  <http://data.europa.eu/r5r/>
PREFIX hvd-ir:  <http://data.europa.eu/eli/reg_impl/2023/138/oj>
PREFIX hvdCategoriesCV: <http://data.europa.eu/bna/asd487ae75>
PREFIX euLicences: <http://publications.europa.eu/resource/authority/licence>
PREFIX countries: <http://publications.europa.eu/resource/authority/country>
PREFIX ccby:    <http://publications.europa.eu/resource/authority/licence/CC_BY_4_0>
PREFIX cc0:     <http://publications.europa.eu/resource/authority/licence/CC0>
"""

# Quality check blocks shared by most reporting queries
QUALITY_BINDS = """
  BIND(EXISTS {
      ?dataset dcat:distribution ?distribution .
      ?distribution a dcat:Distribution ;
        dcatap:applicableLegislation hvd-ir: ;
        dcat:accessURL [] ; dcterms:license ?bdl ; dcat:downloadURL [] .
      OPTIONAL { ?bdl ?bdlmr ?bdleu VALUES ?bdlmr {
        skos:exactMatch skos:broadMatch owl:sameAs skos:narrowMatch skos:closeMatch rdfs:seeAlso } }
      FILTER (?bdl IN (ccby:, cc0:) || ?bdleu IN (ccby:, cc0:))
    } AS ?bulk_download)

  BIND(EXISTS {
      ?dataset dcat:distribution ?distribution .
      ?distribution a dcat:Distribution ;
        dcatap:applicableLegislation hvd-ir: ;
        dcat:accessURL [] ; dcterms:license ?adl ;
        dcat:accessService ?service .
      ?service a dcat:DataService ;
        dcatap:applicableLegislation hvd-ir: ;
        dcat:endpointURL [] ; dcat:contactPoint [] ;
        foaf:page [] ; dcatap:hvdCategory [] .
      OPTIONAL { ?adl ?adlmr ?adleu VALUES ?adlmr {
        skos:exactMatch skos:broadMatch owl:sameAs skos:narrowMatch skos:closeMatch rdfs:seeAlso } }
      FILTER (?adl IN (ccby:, cc0:) || ?adleu IN (ccby:, cc0:))
    } AS ?distribution_service_api)

  BIND(EXISTS {
      ?dataset ^dcat:servesDataset ?service .
      ?service a dcat:DataService ;
        dcatap:applicableLegislation hvd-ir: ;
        dcat:endpointURL [] ; dcat:contactPoint [] ;
        dcterms:license ?sl ; foaf:page [] ; dcatap:hvdCategory [] .
      OPTIONAL { ?sl ?slmr ?sleu VALUES ?slmr {
        skos:exactMatch skos:broadMatch owl:sameAs skos:narrowMatch skos:closeMatch rdfs:seeAlso } }
      FILTER (?sl IN (ccby:, cc0:) || ?sleu IN (ccby:, cc0:))
    } AS ?service_api)

  FILTER(?bulk_download || ?distribution_service_api || ?service_api)
"""

# Q8: count of HVD datasets per country WITHOUT quality checks
Q8 = PREFIXES + """
SELECT DISTINCT ?country (COUNT(DISTINCT ?dataset) AS ?datasets)
WHERE {
  ?catalog dcterms:spatial ?country ;
           dcat:dataset ?dataset .
  ?dataset a dcat:Dataset ;
           dcatap:applicableLegislation hvd-ir: ;
           dcatap:hvdCategory ?HVDCategory .
}
GROUP BY ?country
ORDER BY ?country
"""

# Q7: count of HVD datasets per country WITH quality checks
Q7 = PREFIXES + """
SELECT ?country (COUNT(DISTINCT ?dataset) AS ?datasets)
WHERE {
  {
    SELECT DISTINCT ?country ?dataset
    WHERE {
      ?catalog a dcat:Catalog ; dcterms:spatial ?country ; dcat:dataset ?dataset .
      ?catalogRecord a dcat:CatalogRecord ; foaf:primaryTopic ?dataset ; dcterms:identifier ?identifier .
      ?dataset a dcat:Dataset ;
        dcatap:applicableLegislation hvd-ir: ;
        dcatap:hvdCategory ?HVDCategory .
      GRAPH hvdCategoriesCV: {
        ?HVDCategory skos:inScheme hvdCategoriesCV: .
        FILTER NOT EXISTS { [] skos:broader ?HVDCategory }
      }
      GRAPH countries: {
        ?country skos:prefLabel ?countryName .
        FILTER (langMatches(LANG(?countryName), "en"))
      }
      ?dataset dcterms:title ?title . FILTER (langMatches(LANG(?title), "en"))
      ?dataset dcterms:description ?description . FILTER (langMatches(LANG(?description), "en"))
""" + QUALITY_BINDS + """
    }
  }
}
GROUP BY ?country
ORDER BY ?country
"""

# Q5-nz: granular HVD category coverage per country (no zeros)
Q5_NZ = PREFIXES + """
SELECT DISTINCT ?country ?countryName ?HVDCategory ?HVDCategoryName (COUNT(DISTINCT ?dataset) AS ?datasets)
WHERE {
  ?catalog a dcat:Catalog ; dcterms:spatial ?country ; dcat:dataset ?dataset .
  ?catalogRecord a dcat:CatalogRecord ; foaf:primaryTopic ?dataset ; dcterms:identifier ?identifier .
  ?dataset a dcat:Dataset ;
    dcatap:applicableLegislation hvd-ir: ;
    dcatap:hvdCategory ?HVDCategory .
  GRAPH hvdCategoriesCV: {
    ?HVDCategory skos:inScheme hvdCategoriesCV: ;
                 skos:prefLabel ?HVDCategoryName .
    FILTER (langMatches(LANG(?HVDCategoryName), "en"))
    FILTER NOT EXISTS { ?lowerCategory skos:broader ?HVDCategory }
  }
  GRAPH countries: {
    ?country skos:prefLabel ?countryName .
    FILTER (langMatches(LANG(?countryName), "en"))
  }
  ?dataset dcterms:title ?title . FILTER (langMatches(LANG(?title), "en"))
  ?dataset dcterms:description ?description . FILTER (langMatches(LANG(?description), "en"))
""" + QUALITY_BINDS + """
}
GROUP BY ?country ?countryName ?HVDCategory ?HVDCategoryName
ORDER BY ?country ?countryName ?HVDCategory ?HVDCategoryName
"""

# Q5: granular HVD category coverage per country (including zeros)
Q5 = PREFIXES + """
SELECT DISTINCT ?country ?countryName ?HVDCategory ?HVDCategoryName (MAX(?datasets_or_zero) AS ?datasets)
WHERE {
  {
    SELECT DISTINCT ?country ?countryName ?HVDCategory ?HVDCategoryName (0 AS ?datasets_or_zero)
    WHERE {
      ?catalog a dcat:Catalog ; dcterms:spatial ?country ; dcat:dataset ?dataset .
      ?catalogRecord a dcat:CatalogRecord ; foaf:primaryTopic ?dataset ; dcterms:identifier ?identifier .
      ?dataset a dcat:Dataset ; dcatap:applicableLegislation hvd-ir: .
      GRAPH hvdCategoriesCV: {
        ?HVDCategory skos:inScheme hvdCategoriesCV: ;
                     skos:prefLabel ?HVDCategoryName .
        FILTER (langMatches(LANG(?HVDCategoryName), "en"))
        FILTER NOT EXISTS { ?lowerCategory skos:broader ?HVDCategory }
      }
      GRAPH countries: {
        ?country skos:prefLabel ?countryName .
        FILTER (langMatches(LANG(?countryName), "en"))
      }
      ?dataset dcterms:title ?title . FILTER (langMatches(LANG(?title), "en"))
      ?dataset dcterms:description ?description . FILTER (langMatches(LANG(?description), "en"))
""" + QUALITY_BINDS + """
    }
  }
  UNION
  {
    SELECT DISTINCT ?country ?countryName ?HVDCategory ?HVDCategoryName (COUNT(DISTINCT ?dataset) AS ?datasets_or_zero)
    WHERE {
      ?catalog a dcat:Catalog ; dcterms:spatial ?country ; dcat:dataset ?dataset .
      ?catalogRecord a dcat:CatalogRecord ; foaf:primaryTopic ?dataset ; dcterms:identifier ?identifier .
      ?dataset a dcat:Dataset ;
        dcatap:applicableLegislation hvd-ir: ;
        dcatap:hvdCategory ?HVDCategory .
      GRAPH hvdCategoriesCV: {
        ?HVDCategory skos:inScheme hvdCategoriesCV: ;
                     skos:prefLabel ?HVDCategoryName .
        FILTER (langMatches(LANG(?HVDCategoryName), "en"))
        FILTER NOT EXISTS { ?lowerCategory skos:broader ?HVDCategory }
      }
      GRAPH countries: {
        ?country skos:prefLabel ?countryName .
        FILTER (langMatches(LANG(?countryName), "en"))
      }
      ?dataset dcterms:title ?title . FILTER (langMatches(LANG(?title), "en"))
      ?dataset dcterms:description ?description . FILTER (langMatches(LANG(?description), "en"))
""" + QUALITY_BINDS + """
    }
    GROUP BY ?country ?countryName ?HVDCategory ?HVDCategoryName
  }
}
GROUP BY ?country ?countryName ?HVDCategory ?HVDCategoryName
ORDER BY ?country ?countryName ?HVDCategory ?HVDCategoryName
"""

# Q6-nz: top-level HVD category coverage per country (no zeros)
Q6_NZ = PREFIXES + """
SELECT DISTINCT ?country ?countryName ?HVDCategory ?HVDCategoryName (COUNT(DISTINCT ?dataset) AS ?datasets)
WHERE {
  ?catalog a dcat:Catalog ; dcterms:spatial ?country ; dcat:dataset ?dataset .
  ?catalogRecord a dcat:CatalogRecord ; foaf:primaryTopic ?dataset ; dcterms:identifier ?identifier .
  ?dataset a dcat:Dataset ;
    dcatap:applicableLegislation hvd-ir: ;
    dcatap:hvdCategory ?GranularHVD .
  GRAPH hvdCategoriesCV: {
    ?GranularHVD skos:broader* ?HVDCategory .
    ?HVDCategory skos:prefLabel ?HVDCategoryName .
    FILTER NOT EXISTS { ?lowerCategory skos:broader ?GranularHVD }
    FILTER NOT EXISTS { ?HVDCategory skos:broader ?higherCategory }
    FILTER (langMatches(LANG(?HVDCategoryName), "en"))
  }
  GRAPH countries: {
    ?country skos:prefLabel ?countryName .
    FILTER (langMatches(LANG(?countryName), "en"))
  }
  ?dataset dcterms:title ?title . FILTER (langMatches(LANG(?title), "en"))
  ?dataset dcterms:description ?description . FILTER (langMatches(LANG(?description), "en"))
""" + QUALITY_BINDS + """
}
GROUP BY ?country ?countryName ?HVDCategory ?HVDCategoryName
ORDER BY ?country ?countryName ?HVDCategory ?HVDCategoryName
"""

# Q6: top-level HVD category coverage per country (including zeros)
Q6 = PREFIXES + """
SELECT DISTINCT ?country ?countryName ?HVDCategory ?HVDCategoryName (MAX(?datasets_or_zero) AS ?datasets)
WHERE {
  {
    SELECT DISTINCT ?country ?countryName ?HVDCategory ?HVDCategoryName (0 AS ?datasets_or_zero)
    WHERE {
      ?catalog a dcat:Catalog ; dcterms:spatial ?country ; dcat:dataset ?dataset .
      ?catalogRecord a dcat:CatalogRecord ; foaf:primaryTopic ?dataset ; dcterms:identifier ?identifier .
      ?dataset a dcat:Dataset ; dcatap:applicableLegislation hvd-ir: .
      GRAPH hvdCategoriesCV: {
        ?HVDCategory skos:inScheme hvdCategoriesCV: ;
                     skos:prefLabel ?HVDCategoryName .
        FILTER (langMatches(LANG(?HVDCategoryName), "en"))
        FILTER NOT EXISTS { ?HVDCategory skos:broader ?higherCategory }
      }
      GRAPH countries: {
        ?country skos:prefLabel ?countryName .
        FILTER (langMatches(LANG(?countryName), "en"))
      }
      ?dataset dcterms:title ?title . FILTER (langMatches(LANG(?title), "en"))
      ?dataset dcterms:description ?description . FILTER (langMatches(LANG(?description), "en"))
""" + QUALITY_BINDS + """
    }
  }
  UNION
  {
    SELECT DISTINCT ?country ?countryName ?HVDCategory ?HVDCategoryName (COUNT(DISTINCT ?dataset) AS ?datasets_or_zero)
    WHERE {
      ?catalog a dcat:Catalog ; dcterms:spatial ?country ; dcat:dataset ?dataset .
      ?catalogRecord a dcat:CatalogRecord ; foaf:primaryTopic ?dataset ; dcterms:identifier ?identifier .
      ?dataset a dcat:Dataset ;
        dcatap:applicableLegislation hvd-ir: ;
        dcatap:hvdCategory ?GranularHVD .
      GRAPH hvdCategoriesCV: {
        ?GranularHVD skos:broader* ?HVDCategory .
        ?HVDCategory skos:prefLabel ?HVDCategoryName .
        FILTER NOT EXISTS { ?lowerCategory skos:broader ?GranularHVD }
        FILTER NOT EXISTS { ?HVDCategory skos:broader ?higherCategory }
        FILTER (langMatches(LANG(?HVDCategoryName), "en"))
      }
      GRAPH countries: {
        ?country skos:prefLabel ?countryName .
        FILTER (langMatches(LANG(?countryName), "en"))
      }
      ?dataset dcterms:title ?title . FILTER (langMatches(LANG(?title), "en"))
      ?dataset dcterms:description ?description . FILTER (langMatches(LANG(?description), "en"))
""" + QUALITY_BINDS + """
    }
    GROUP BY ?country ?countryName ?HVDCategory ?HVDCategoryName
  }
}
GROUP BY ?country ?countryName ?HVDCategory ?HVDCategoryName
ORDER BY ?country ?countryName ?HVDCategory ?HVDCategoryName
"""

# Q-MS1: list of valid HVD datasets per country
Q_MS1 = PREFIXES + """
SELECT DISTINCT ?country ?identifier (?dataset AS ?EDPidentifier)
WHERE {
  ?catalog a dcat:Catalog ; dcterms:spatial ?country ; dcat:dataset ?dataset .
  ?catalogRecord a dcat:CatalogRecord ; foaf:primaryTopic ?dataset ; dcterms:identifier ?identifier .
  ?dataset a dcat:Dataset ;
    dcatap:applicableLegislation hvd-ir: ;
    dcatap:hvdCategory ?HVDCategory .
  GRAPH hvdCategoriesCV: {
    ?HVDCategory skos:inScheme hvdCategoriesCV: .
    FILTER NOT EXISTS { ?lowerCategory skos:broader ?HVDCategory }
  }
  ?dataset dcterms:title ?title . FILTER (langMatches(LANG(?title), "en"))
  ?dataset dcterms:description ?description . FILTER (langMatches(LANG(?description), "en"))
""" + QUALITY_BINDS + """
}
ORDER BY ?country ?identifier
"""

# Q-MS2: datasets per country with full category hierarchy (quality filtered)
Q_MS2 = PREFIXES + """
SELECT DISTINCT ?country ?countryName ?HVDCategoryTop ?HVDCategoryTopName ?HVDCategory ?HVDCategoryName
       ?identifier (?dataset AS ?EDPidentifier) ?title ?description
       ?bulk_download ?distribution_service_api ?service_api
       ((?distribution_service_api || ?service_api) AS ?api)
WHERE {
  ?catalog a dcat:Catalog ; dcterms:spatial ?country ; dcat:dataset ?dataset .
  ?catalogRecord a dcat:CatalogRecord ; foaf:primaryTopic ?dataset ; dcterms:identifier ?identifier .
  ?dataset a dcat:Dataset ;
    dcatap:applicableLegislation hvd-ir: ;
    dcatap:hvdCategory ?HVDCategory .
  GRAPH hvdCategoriesCV: {
    ?HVDCategory skos:inScheme hvdCategoriesCV: .
    FILTER NOT EXISTS { [] skos:broader ?HVDCategory }
    ?HVDCategory skos:broader* ?HVDCategoryTop ;
                 skos:prefLabel ?HVDCategoryName .
    ?HVDCategoryTop skos:prefLabel ?HVDCategoryTopName .
    FILTER NOT EXISTS { ?HVDCategoryTop skos:broader [] }
    FILTER (langMatches(LANG(?HVDCategoryName), "en"))
    FILTER (langMatches(LANG(?HVDCategoryTopName), "en"))
  }
  GRAPH countries: {
    ?country skos:prefLabel ?countryName .
    FILTER (langMatches(LANG(?countryName), "en"))
  }
  ?dataset dcterms:title ?title . FILTER (langMatches(LANG(?title), "en"))
  ?dataset dcterms:description ?description . FILTER (langMatches(LANG(?description), "en"))
""" + QUALITY_BINDS + """
}
ORDER BY ?country ?HVDCategoryTop ?HVDCategory ?identifier
"""

# Q-MS2-no-filter: datasets per country with category hierarchy (no quality filter)
Q_MS2_NO_FILTER = PREFIXES + """
SELECT DISTINCT ?country ?countryName ?HVDCategoryTop ?HVDCategoryTopName ?HVDCategory ?HVDCategoryName
       ?identifier (?dataset AS ?EDPidentifier) ?title ?description
       ?bulk_download ?distribution_service_api ?service_api
       ((?distribution_service_api || ?service_api) AS ?api)
WHERE {
  ?catalog a dcat:Catalog ; dcterms:spatial ?country ; dcat:dataset ?dataset .
  ?catalogRecord a dcat:CatalogRecord ; foaf:primaryTopic ?dataset ; dcterms:identifier ?identifier .
  ?dataset a dcat:Dataset ;
    dcatap:applicableLegislation hvd-ir: ;
    dcatap:hvdCategory ?HVDCategory .
  GRAPH hvdCategoriesCV: {
    ?HVDCategory skos:inScheme hvdCategoriesCV: .
    FILTER NOT EXISTS { [] skos:broader ?HVDCategory }
    ?HVDCategory skos:broader* ?HVDCategoryTop ;
                 skos:prefLabel ?HVDCategoryName .
    ?HVDCategoryTop skos:prefLabel ?HVDCategoryTopName .
    FILTER NOT EXISTS { ?HVDCategoryTop skos:broader [] }
    FILTER (langMatches(LANG(?HVDCategoryName), "en"))
    FILTER (langMatches(LANG(?HVDCategoryTopName), "en"))
  }
  GRAPH countries: {
    ?country skos:prefLabel ?countryName .
    FILTER (langMatches(LANG(?countryName), "en"))
  }
  ?dataset dcterms:title ?title . FILTER (langMatches(LANG(?title), "en"))
  ?dataset dcterms:description ?description . FILTER (langMatches(LANG(?description), "en"))

  BIND(EXISTS {
      ?dataset dcat:distribution ?distribution .
      ?distribution a dcat:Distribution ;
        dcatap:applicableLegislation hvd-ir: ;
        dcat:accessURL [] ; dcterms:license ?bdl ; dcat:downloadURL [] .
      OPTIONAL { ?bdl ?bdlmr ?bdleu VALUES ?bdlmr {
        skos:exactMatch skos:broadMatch owl:sameAs skos:narrowMatch skos:closeMatch rdfs:seeAlso } }
      FILTER (?bdl IN (ccby:, cc0:) || ?bdleu IN (ccby:, cc0:))
    } AS ?bulk_download)

  BIND(EXISTS {
      ?dataset dcat:distribution ?distribution .
      ?distribution a dcat:Distribution ;
        dcatap:applicableLegislation hvd-ir: ;
        dcat:accessURL [] ; dcterms:license ?adl ;
        dcat:accessService ?dservice .
      ?dservice a dcat:DataService ;
        dcatap:applicableLegislation hvd-ir: ;
        dcat:endpointURL [] ; dcat:contactPoint [] ;
        foaf:page [] ; dcatap:hvdCategory [] .
      OPTIONAL { ?adl ?adlmr ?adleu VALUES ?adlmr {
        skos:exactMatch skos:broadMatch owl:sameAs skos:narrowMatch skos:closeMatch rdfs:seeAlso } }
      FILTER (?adl IN (ccby:, cc0:) || ?adleu IN (ccby:, cc0:))
    } AS ?distribution_service_api)

  BIND(EXISTS {
      ?dataset ^dcat:servesDataset ?service .
      ?service a dcat:DataService ;
        dcatap:applicableLegislation hvd-ir: ;
        dcat:endpointURL [] ; dcat:contactPoint [] ;
        dcterms:license ?sl ; foaf:page [] ; dcatap:hvdCategory [] .
      OPTIONAL { ?sl ?slmr ?sleu VALUES ?slmr {
        skos:exactMatch skos:broadMatch owl:sameAs skos:narrowMatch skos:closeMatch rdfs:seeAlso } }
      FILTER (?sl IN (ccby:, cc0:) || ?sleu IN (ccby:, cc0:))
    } AS ?service_api)
  # No final FILTER — returns all datasets regardless of access/licence validity
}
ORDER BY ?country ?HVDCategoryTop ?HVDCategory ?identifier
"""

# Q-MS3: API links per dataset per country
Q_MS3 = PREFIXES + """
SELECT DISTINCT ?country ?countryName ?identifier ?apiDistributionIdentifier ?apiDistribution
       ?service ?serviceEndpointURL ?QoSDocument ?contactPoint
       ?contactPointPage ?contactPointEmail ?license ?licenseMappingRelation ?EULicense
WHERE {
  ?catalog a dcat:Catalog ; dcterms:spatial ?country ; dcat:dataset ?dataset .
  ?catalogRecord a dcat:CatalogRecord ; foaf:primaryTopic ?dataset ; dcterms:identifier ?identifier .
  ?dataset a dcat:Dataset ;
    dcatap:applicableLegislation hvd-ir: ;
    dcatap:hvdCategory ?HVDCategory .
  GRAPH hvdCategoriesCV: {
    ?HVDCategory skos:inScheme hvdCategoriesCV: .
    FILTER NOT EXISTS { [] skos:broader ?HVDCategory }
  }
  GRAPH countries: {
    ?country skos:prefLabel ?countryName .
    FILTER (langMatches(LANG(?countryName), "en"))
  }
  ?dataset dcterms:title ?title . FILTER (langMatches(LANG(?title), "en"))
  ?dataset dcterms:description ?description . FILTER (langMatches(LANG(?description), "en"))

  {
    ?dataset dcat:distribution ?apiDistribution .
    ?apiDistribution a dcat:Distribution ;
      dcatap:applicableLegislation hvd-ir: ;
      dcterms:identifier ?apiDistributionIdentifier ;
      dcat:accessURL ?apiDistributionAccessURL ;
      dcterms:license ?license ;
      dcat:accessService ?service .
    ?service a dcat:DataService ;
      dcatap:applicableLegislation hvd-ir: ;
      dcat:endpointURL ?serviceEndpointURL ;
      dcat:contactPoint ?contactPoint ;
      foaf:page ?QoSDocument ;
      dcatap:hvdCategory ?distributionServiceHVDCategory .
    GRAPH hvdCategoriesCV: {
      ?distributionServiceHVDCategory skos:inScheme hvdCategoriesCV: .
      FILTER NOT EXISTS { [] skos:broader ?distributionServiceHVDCategory }
    }
  }
  UNION
  {
    ?dataset ^dcat:servesDataset ?service .
    ?service a dcat:DataService ;
      dcatap:applicableLegislation hvd-ir: ;
      dcat:endpointURL ?serviceEndpointURL ;
      dcat:contactPoint ?contactPoint ;
      dcterms:license ?license ;
      foaf:page ?QoSDocument ;
      dcatap:hvdCategory ?serviceHVDCategory .
    GRAPH hvdCategoriesCV: {
      ?serviceHVDCategory skos:inScheme hvdCategoriesCV: .
      FILTER NOT EXISTS { [] skos:broader ?serviceHVDCategory }
    }
  }
  OPTIONAL {
    ?license ?licenseMappingRelation ?EULicense
    VALUES ?licenseMappingRelation {
      skos:exactMatch skos:broadMatch owl:sameAs skos:narrowMatch skos:closeMatch rdfs:seeAlso }
  }
  FILTER (?license IN (ccby:, cc0:) || ?EULicense IN (ccby:, cc0:))
  OPTIONAL { ?contactPoint vcard:hasURL ?contactPointPage . }
  OPTIONAL { ?contactPoint vcard:hasEmail ?contactPointEmail . }
""" + QUALITY_BINDS + """
}
ORDER BY ?country ?identifier
"""

# Q-MS4: bulk download links per dataset per country
Q_MS4 = PREFIXES + """
SELECT DISTINCT ?country ?countryName ?identifier ?distributionIdentifier
       ?distributionBulk ?bulkDownloadAccessURL ?bulkDownloadDownloadURL
       ?license ?licenseMappingRelation ?EULicense
WHERE {
  ?catalog a dcat:Catalog ; dcterms:spatial ?country ; dcat:dataset ?dataset .
  ?catalogRecord a dcat:CatalogRecord ; foaf:primaryTopic ?dataset ; dcterms:identifier ?identifier .
  ?dataset a dcat:Dataset ;
    dcatap:applicableLegislation hvd-ir: ;
    dcatap:hvdCategory ?HVDCategory ;
    dcat:distribution ?distributionBulk .
  GRAPH hvdCategoriesCV: {
    ?HVDCategory skos:inScheme hvdCategoriesCV: .
    FILTER NOT EXISTS { [] skos:broader ?HVDCategory }
  }
  GRAPH countries: {
    ?country skos:prefLabel ?countryName .
    FILTER (langMatches(LANG(?countryName), "en"))
  }
  ?dataset dcterms:title ?title . FILTER (langMatches(LANG(?title), "en"))
  ?dataset dcterms:description ?description . FILTER (langMatches(LANG(?description), "en"))

  ?distributionBulk a dcat:Distribution ;
    dcterms:identifier ?distributionIdentifier ;
    dcatap:applicableLegislation hvd-ir: ;
    dcat:accessURL ?bulkDownloadAccessURL ;
    dcterms:license ?license ;
    dcat:downloadURL ?bulkDownloadDownloadURL .

  OPTIONAL {
    ?license ?licenseMappingRelation ?EULicense
    VALUES ?licenseMappingRelation {
      skos:exactMatch skos:broadMatch owl:sameAs skos:narrowMatch skos:closeMatch rdfs:seeAlso }
  }
  FILTER (?license IN (ccby:, cc0:) || ?EULicense IN (ccby:, cc0:))
""" + QUALITY_BINDS + """
}
ORDER BY ?country ?identifier
"""

# (output filename, query) - filenames are the ones the reporting app expects
ALL_QUERIES = [
    ("Q8 - Current numbers of HVDs in countries - without quality checks.csv", Q8),
    ("Q7 - Current numbers of HVDs in countries with quality checks.csv", Q7),
    ("Q5-nz.csv", Q5_NZ),
    ("Q5 - HVD granular category-dataset coverage per country.csv", Q5),
    ("Q6-nz.csv", Q6_NZ),
    ("Q6 - HVD top level category coverage per country.csv", Q6),
    ("Q-MS1.csv", Q_MS1),
    ("Q-MS2.csv", Q_MS2),
    ("Q-MS2-no-filter.csv", Q_MS2_NO_FILTER),
    ("Q-MS3.csv", Q_MS3),
    ("Q-MS4.csv", Q_MS4),
]

# ---------------------------------------------------------------------------
# Phase C - per-catalogue compliance checks for the reporting dashboard
#
# The dashboard (HVD_REPORTING_TOOL_V2) fetches
#   dashboard/data/compliance_<catalogue_id>.json
# and reads summary.total_datasets / summary.total_apis plus the counts of
# the check ids below (see applyComplianceData in hvd_dashboard.js).
# ---------------------------------------------------------------------------

# (id, label, subject, severity, article, description) - schema and wording
# follow the example file shipped with the dashboard.
CHECK_DEFS = [
    ("datasets_total", "Total HVD-tagged datasets", "dataset", "info", None,
     "Total number of datasets tagged with the HVD implementing regulation."),
    ("datasets_no_title", "Datasets with no title", "dataset", "recommended", "Art. 3",
     "Datasets missing dct:title entirely."),
    ("datasets_no_title_en", "Datasets with no English title", "dataset", "recommended", "Art. 3",
     "Datasets missing dct:title with language tag @en."),
    ("datasets_no_description", "Datasets with no description", "dataset", "recommended", "Art. 3",
     "Datasets missing dct:description entirely."),
    ("datasets_no_description_en", "Datasets with no English description", "dataset", "recommended", "Art. 3",
     "Datasets missing dct:description with language tag @en."),
    ("datasets_no_category", "Datasets with no HVD category", "dataset", "mandatory", "Art. 2",
     "Datasets missing r5r:hvdCategory entirely."),
    ("datasets_no_granular_category", "Datasets with no granular HVD category", "dataset", "mandatory", "Art. 2",
     "Datasets where the hvdCategory is a top-level category instead of the most granular sub-category."),
    ("datasets_no_hvd_distribution", "Datasets with no HVD-tagged distribution", "dataset", "mandatory", "Art. 4",
     "Datasets that have distributions but none are tagged with r5r:applicableLegislation hvd-ir:."),
    ("datasets_no_api", "Datasets with no HVD-tagged API", "dataset", "mandatory", "Art. 5",
     "Datasets with no associated HVD-tagged API. Note: not a mandatory issue if the dataset "
     "has a valid HVD bulk download distribution."),
    ("distributions_no_access_url", "Distributions with no accessURL", "distribution", "mandatory", "Art. 4",
     "HVD-tagged distributions missing dcat:accessURL."),
    ("distributions_no_download_url", "Bulk download distributions with no downloadURL", "distribution", "mandatory", "Art. 4",
     "HVD-tagged distributions that are bulk downloads (no HVD-tagged accessService) but are "
     "missing dcat:downloadURL."),
    ("distributions_no_licence", "Distributions with no licence", "distribution", "recommended", "Art. 4",
     "HVD-tagged distributions missing dct:license. May not constitute a mandatory issue if the "
     "dataset is served by an HVD API that provides licence at service level."),
    ("bulk_downloads_no_eu_licence", "Bulk downloads with no open EU licence or mapping", "distribution", "mandatory", "Art. 4",
     "HVD bulk download distributions where the licence is not CC-BY 4.0 or CC0 and is not "
     "mapped to those via skos:exactMatch or equivalent."),
    ("apis_total", "Total HVD-tagged APIs", "api", "info", "Art. 5",
     "Total number of HVD-tagged APIs (via distribution accessService or dcat:servesDataset)."),
    ("apis_no_endpoint_url", "APIs with no endpoint URL", "api", "mandatory", "Art. 5",
     "HVD-tagged APIs missing dcat:endpointURL."),
    ("apis_no_contact_point", "APIs with no contact point", "api", "mandatory", "Art. 5",
     "HVD-tagged APIs missing dcat:contactPoint."),
    ("apis_no_qos_document", "APIs with no QoS document", "api", "mandatory", "Art. 5",
     "HVD-tagged APIs missing foaf:page (quality of service document)."),
    ("apis_no_hvd_category", "APIs with no HVD category", "api", "mandatory", "Art. 5",
     "HVD-tagged APIs missing r5r:hvdCategory."),
    ("apis_no_licence", "APIs with no licence", "api", "mandatory", "Art. 5(6)",
     "HVD-tagged APIs missing dct:license."),
    ("apis_no_eu_licence", "APIs with no open EU licence or mapping", "api", "mandatory", "Art. 5(6)",
     "HVD-tagged APIs where the licence is not CC-BY 4.0 or CC0 and is not mapped to those via "
     "skos:exactMatch or equivalent."),
]

_OPEN_LICENCE_EXISTS = """EXISTS {
      %(node)s dcterms:license ?lic_%(tag)s .
      OPTIONAL { ?lic_%(tag)s ?rel_%(tag)s ?eu_%(tag)s VALUES ?rel_%(tag)s {
        skos:exactMatch skos:broadMatch owl:sameAs skos:narrowMatch skos:closeMatch rdfs:seeAlso } }
      FILTER (?lic_%(tag)s IN (ccby:, cc0:) || ?eu_%(tag)s IN (ccby:, cc0:))
    }"""

# One row per HVD dataset with per-dataset boolean flags.
Q_DATASET_FLAGS = PREFIXES + """
SELECT DISTINCT ?catalog ?country ?dataset
       ?no_title ?has_title_en ?no_description ?has_description_en
       ?has_category ?has_granular_category ?has_distribution
       ?has_hvd_distribution ?has_api
WHERE {
  ?catalog a dcat:Catalog ; dcterms:spatial ?country ; dcat:dataset ?dataset .
  ?dataset a dcat:Dataset ;
           dcatap:applicableLegislation hvd-ir: .

  BIND(EXISTS { ?dataset <urn:x-hvd-etl:missingTitle> true } AS ?no_title)
  BIND(EXISTS { ?dataset dcterms:title ?t .
                FILTER (langMatches(LANG(?t), "en")) } AS ?has_title_en)
  BIND(EXISTS { ?dataset <urn:x-hvd-etl:missingDescription> true } AS ?no_description)
  BIND(EXISTS { ?dataset dcterms:description ?d .
                FILTER (langMatches(LANG(?d), "en")) } AS ?has_description_en)
  BIND(EXISTS { ?dataset dcatap:hvdCategory ?anyCat } AS ?has_category)
  BIND(EXISTS { ?dataset dcatap:hvdCategory ?granCat .
                ?granCat skos:inScheme hvdCategoriesCV: .
                FILTER NOT EXISTS { ?narrower skos:broader ?granCat }
              } AS ?has_granular_category)
  BIND(EXISTS { ?dataset dcat:distribution ?anyDist } AS ?has_distribution)
  BIND(EXISTS { ?dataset dcat:distribution ?hvdDist .
                ?hvdDist dcatap:applicableLegislation hvd-ir: } AS ?has_hvd_distribution)
  BIND(EXISTS { ?dataset (dcat:distribution/dcat:accessService)|^dcat:servesDataset ?svc .
                ?svc a dcat:DataService ;
                     dcatap:applicableLegislation hvd-ir: } AS ?has_api)
}
"""

# One row per HVD-tagged distribution of an HVD dataset.
Q_DISTRIBUTION_FLAGS = PREFIXES + """
SELECT DISTINCT ?catalog ?distribution
       ?has_access_url ?has_download_url ?has_licence
       ?has_hvd_service ?has_open_licence
WHERE {
  ?catalog a dcat:Catalog ; dcterms:spatial ?country ; dcat:dataset ?dataset .
  ?dataset a dcat:Dataset ;
           dcatap:applicableLegislation hvd-ir: ;
           dcat:distribution ?distribution .
  ?distribution a dcat:Distribution ;
                dcatap:applicableLegislation hvd-ir: .

  BIND(EXISTS { ?distribution dcat:accessURL ?au } AS ?has_access_url)
  BIND(EXISTS { ?distribution dcat:downloadURL ?du } AS ?has_download_url)
  BIND(EXISTS { ?distribution dcterms:license ?li } AS ?has_licence)
  BIND(EXISTS { ?distribution dcat:accessService ?ds_ .
                ?ds_ dcatap:applicableLegislation hvd-ir: } AS ?has_hvd_service)
  BIND(""" + _OPEN_LICENCE_EXISTS % {"node": "?distribution", "tag": "d"} + """ AS ?has_open_licence)
}
"""

# One row per HVD-tagged API (data service) linked to an HVD dataset.
Q_SERVICE_FLAGS = PREFIXES + """
SELECT DISTINCT ?catalog ?service
       ?has_endpoint_url ?has_contact_point ?has_qos_document
       ?has_category ?has_licence ?has_open_licence
WHERE {
  ?catalog a dcat:Catalog ; dcterms:spatial ?country ; dcat:dataset ?dataset .
  ?dataset a dcat:Dataset ;
           dcatap:applicableLegislation hvd-ir: .
  ?dataset (dcat:distribution/dcat:accessService)|^dcat:servesDataset ?service .
  ?service a dcat:DataService ;
           dcatap:applicableLegislation hvd-ir: .

  BIND(EXISTS { ?service dcat:endpointURL ?ep } AS ?has_endpoint_url)
  BIND(EXISTS { ?service dcat:contactPoint ?cp } AS ?has_contact_point)
  BIND(EXISTS { ?service foaf:page ?pg } AS ?has_qos_document)
  BIND(EXISTS { ?service dcatap:hvdCategory ?sc } AS ?has_category)
  BIND(EXISTS { ?service dcterms:license ?sl } AS ?has_licence)
  BIND(""" + _OPEN_LICENCE_EXISTS % {"node": "?service", "tag": "s"} + """ AS ?has_open_licence)
}
"""

# Catalogue country names, for the catalogues.json manifest.
Q_CATALOG_COUNTRIES = PREFIXES + """
SELECT DISTINCT ?catalog ?country ?countryName
WHERE {
  ?catalog a dcat:Catalog ; dcterms:spatial ?country .
  GRAPH countries: {
    ?country skos:prefLabel ?countryName .
    FILTER (langMatches(LANG(?countryName), "en"))
  }
}
"""


def _as_bool(value):
    return str(value).lower() == "true"


def catalogue_id_from_iri(iri):
    return str(iri).rstrip("/").rsplit("/", 1)[-1]


def compute_compliance(ds):
    """Aggregate the flag queries into per-catalogue check counts.

    Returns {catalogue_iri: {"country": ..., "counts": {check_id: n}}}.
    """
    per_cat = {}

    def cat_entry(catalog):
        key = str(catalog)
        if key not in per_cat:
            per_cat[key] = {"country": None, "country_name": None,
                            "counts": {cid: 0 for cid, *_ in CHECK_DEFS}}
        return per_cat[key]

    log.info("Computing compliance flags per dataset...")
    for row in ds.query(Q_DATASET_FLAGS):
        c = cat_entry(row.catalog)
        c["country"] = str(row.country)
        counts = c["counts"]
        counts["datasets_total"] += 1
        counts["datasets_no_title"] += _as_bool(row.no_title)
        counts["datasets_no_title_en"] += not _as_bool(row.has_title_en)
        counts["datasets_no_description"] += _as_bool(row.no_description)
        counts["datasets_no_description_en"] += not _as_bool(row.has_description_en)
        counts["datasets_no_category"] += not _as_bool(row.has_category)
        # only flag a non-granular category when a category is present at all;
        # datasets without any category are already counted above
        counts["datasets_no_granular_category"] += (
            _as_bool(row.has_category) and not _as_bool(row.has_granular_category))
        counts["datasets_no_hvd_distribution"] += (
            _as_bool(row.has_distribution) and not _as_bool(row.has_hvd_distribution))
        counts["datasets_no_api"] += not _as_bool(row.has_api)

    log.info("Computing compliance flags per distribution...")
    for row in ds.query(Q_DISTRIBUTION_FLAGS):
        counts = cat_entry(row.catalog)["counts"]
        counts["distributions_no_access_url"] += not _as_bool(row.has_access_url)
        counts["distributions_no_licence"] += not _as_bool(row.has_licence)
        is_bulk = not _as_bool(row.has_hvd_service)
        if is_bulk:
            counts["distributions_no_download_url"] += not _as_bool(row.has_download_url)
            counts["bulk_downloads_no_eu_licence"] += (
                _as_bool(row.has_licence) and not _as_bool(row.has_open_licence))

    log.info("Computing compliance flags per API...")
    for row in ds.query(Q_SERVICE_FLAGS):
        counts = cat_entry(row.catalog)["counts"]
        counts["apis_total"] += 1
        counts["apis_no_endpoint_url"] += not _as_bool(row.has_endpoint_url)
        counts["apis_no_contact_point"] += not _as_bool(row.has_contact_point)
        counts["apis_no_qos_document"] += not _as_bool(row.has_qos_document)
        counts["apis_no_hvd_category"] += not _as_bool(row.has_category)
        counts["apis_no_licence"] += not _as_bool(row.has_licence)
        counts["apis_no_eu_licence"] += (
            _as_bool(row.has_licence) and not _as_bool(row.has_open_licence))

    for row in ds.query(Q_CATALOG_COUNTRIES):
        key = str(row.catalog)
        if key in per_cat:
            per_cat[key]["country_name"] = str(row.countryName)

    return per_cat


def write_compliance_files(ds, output_dir, extra_dir=None):
    """Write compliance_<catalogue_id>.json per catalogue plus a
    catalogues.json manifest into <output_dir>/dashboard/data (and optionally
    into extra_dir, e.g. the app checkout's dashboard/data)."""
    per_cat = compute_compliance(ds)
    if not per_cat:
        log.warning("No catalogues found — skipping compliance JSON files.")
        return 0

    data_dir = os.path.join(output_dir, "dashboard", "data")
    dirs = [data_dir] + ([extra_dir] if extra_dir else [])
    for d in dirs:
        os.makedirs(d, exist_ok=True)

    generated = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    manifest = []
    for catalogue_iri in sorted(per_cat):
        entry = per_cat[catalogue_iri]
        counts = entry["counts"]
        catalogue_id = catalogue_id_from_iri(catalogue_iri)

        checks = []
        for cid, label, subject, severity, article, description in CHECK_DEFS:
            count = counts[cid]
            if severity == "info":
                status = "info"
            elif count == 0:
                status = "pass"
            else:
                status = severity
            checks.append({
                "id": cid, "label": label, "subject": subject,
                "severity": severity, "article": article,
                "description": description, "count": count, "status": status,
            })

        doc = {
            "catalogue": catalogue_iri,
            "catalogue_id": catalogue_id,
            "generated": generated,
            "summary": {
                "total_datasets": counts["datasets_total"],
                "total_apis": counts["apis_total"],
                "mandatory_count": sum(1 for c in checks if c["status"] == "mandatory"),
                "recommended_count": sum(1 for c in checks if c["status"] == "recommended"),
                "checks_total": len(checks),
                "checks_passing": sum(1 for c in checks if c["status"] == "pass"),
            },
            "checks": checks,
        }
        for d in dirs:
            with open(os.path.join(d, "compliance_%s.json" % catalogue_id), "w",
                      encoding="utf-8") as f:
                json.dump(doc, f, indent=2, ensure_ascii=False)

        manifest.append({
            "catalogue": catalogue_iri,
            "catalogue_id": catalogue_id,
            "country": entry["country"],
            "country_name": entry["country_name"],
            "total_datasets": counts["datasets_total"],
        })

    manifest.sort(key=lambda m: ((m["country_name"] or ""), m["catalogue_id"]))
    for d in dirs:
        with open(os.path.join(d, "catalogues.json"), "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

    log.info("Wrote %d compliance file(s) + catalogues.json to %s",
             len(manifest), " and ".join(dirs))
    return len(manifest)


# ---------------------------------------------------------------------------
# SPARQL helpers
# ---------------------------------------------------------------------------


def run_sparql_select(query, endpoint):
    """SPARQL SELECT against the remote endpoint, returns CSV bytes."""
    data = urlencode({"query": query, "format": "text/csv"}).encode("utf-8")
    req = Request(endpoint, data=data, method="POST",
                  headers={"Content-Type": "application/x-www-form-urlencoded",
                           "Accept": "text/csv"})
    with urlopen(req, timeout=300) as response:
        return response.read()


def run_sparql_construct(query, endpoint):
    """SPARQL CONSTRUCT against the remote endpoint, returns Turtle bytes."""
    data = urlencode({"query": query}).encode("utf-8")
    req = Request(endpoint, data=data, method="POST",
                  headers={"Content-Type": "application/x-www-form-urlencoded",
                           "Accept": "text/turtle"})
    with urlopen(req, timeout=300) as response:
        return response.read()


def results_to_csv(results):
    """rdflib query results -> CSV bytes."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([str(v) for v in results.vars])
    for row in results:
        writer.writerow([str(v) if v is not None else "" for v in row])
    return output.getvalue().encode("utf-8")


def save_csv(csv_bytes, output_dir, filename):
    out_path = os.path.join(output_dir, filename)
    with open(out_path, "wb") as f:
        f.write(csv_bytes)
    row_count = max(0, len(csv_bytes.decode("utf-8").strip().splitlines()) - 1)
    return out_path, row_count


# ---------------------------------------------------------------------------
# Phase A - build the local graph
# ---------------------------------------------------------------------------


def load_vocabularies(ds):
    """Download each controlled vocabulary into its own named graph."""
    log.info("Loading controlled vocabularies into named graphs...")
    for vocab in VOCABULARIES:
        log.info("  [%s] %s", vocab["name"], vocab["graph_iri"])
        req = Request(vocab["url"], headers=HTTP_HEADERS)
        with urlopen(req, timeout=60) as response:
            rdf_data = response.read()
        graph = ds.graph(vocab["graph_iri"])
        graph.parse(io.BytesIO(rdf_data), format=vocab["format"])
        log.info("    %s triples loaded", f"{len(graph):,}")
    log.info("Total triples in Dataset: %s", f"{len(ds):,}")


def get_countries(endpoint):
    """Country IRIs present in the endpoint's catalogs."""
    rows = run_sparql_select(SELECT_COUNTRIES, endpoint).decode("utf-8").strip().splitlines()
    return [r.strip().strip('"') for r in rows[1:] if r.strip()]


def load_hvd_metadata(ds, endpoint):
    """Run the CONSTRUCT steps: global first, per-country on failure.

    Returns a list of "step / country" labels that could not be fetched, so
    the caller can decide whether the run is complete enough.
    """
    hvd_graph = ds.graph(GRAPH_HVD_DATASETS)
    log.info("Target named graph: %s", GRAPH_HVD_DATASETS)
    failures = []
    country_cache = None

    for label, template in CONSTRUCT_STEPS:
        log.info("[%s]", label)
        before = len(hvd_graph)
        query = template.replace("__SPATIAL__", SPATIAL_GLOBAL)
        t0 = time.time()
        try:
            hvd_graph.parse(io.BytesIO(run_sparql_construct(query, endpoint)),
                            format="turtle")
            log.info("  global fetch: +%s triples (%.1fs)",
                     f"{len(hvd_graph) - before:,}", time.time() - t0)
        except Exception as e:
            log.warning("  global fetch FAILED (%s) — falling back to per-country fetch", e)
            if country_cache is None:
                try:
                    country_cache = get_countries(endpoint)
                except Exception as ce:
                    log.error("  could not fetch country list: %s", ce)
                    country_cache = []
            for i, country_iri in enumerate(country_cache, 1):
                short = country_iri.split("/")[-1]
                spatial = ("?catalog a dcat:Catalog ; "
                           "dcterms:spatial <%s> ; dcat:dataset ?dataset ." % country_iri)
                q = template.replace("__SPATIAL__", spatial)
                t0 = time.time()
                try:
                    b = len(hvd_graph)
                    hvd_graph.parse(io.BytesIO(run_sparql_construct(q, endpoint)),
                                    format="turtle")
                    log.info("    [%d/%d] %s: +%s (%.1fs)", i, len(country_cache),
                             short, f"{len(hvd_graph) - b:,}", time.time() - t0)
                except Exception as e2:
                    log.error("    [%d/%d] %s: ERROR %s", i, len(country_cache), short, e2)
                    failures.append("%s / %s" % (label, short))
                time.sleep(1)
        time.sleep(2)

    log.info("HVD datasets graph : %s triples", f"{len(hvd_graph):,}")
    log.info("Total in Dataset   : %s triples", f"{len(ds):,}")
    return failures


def validate(ds):
    """Log per-graph count checks so a broken fetch is visible."""
    checks = [
        (
            "HVD datasets loaded in named graph",
            """
            PREFIX dcat: <http://www.w3.org/ns/dcat#>
            PREFIX dcatap: <http://data.europa.eu/r5r/>
            PREFIX hvd-ir: <http://data.europa.eu/eli/reg_impl/2023/138/oj>
            SELECT (COUNT(DISTINCT ?dataset) AS ?count) WHERE {
                GRAPH <%s> {
                    ?dataset a dcat:Dataset ;
                             dcatap:applicableLegislation hvd-ir: .
                }
            }
            """ % GRAPH_HVD_DATASETS,
        ),
        (
            "Countries in named graph",
            """
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            SELECT (COUNT(DISTINCT ?country) AS ?count) WHERE {
                GRAPH <%s> {
                    ?country skos:prefLabel ?name .
                    FILTER (langMatches(LANG(?name), "en"))
                }
            }
            """ % GRAPH_COUNTRIES,
        ),
        (
            "Granular HVD categories in named graph",
            """
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            SELECT (COUNT(DISTINCT ?cat) AS ?count) WHERE {
                GRAPH <%s> {
                    ?cat skos:inScheme <%s> .
                    FILTER NOT EXISTS { [] skos:broader ?cat }
                }
            }
            """ % (GRAPH_HVD_CATS, GRAPH_HVD_CATS),
        ),
    ]
    log.info("Validation counts across named graphs:")
    for label, query in checks:
        result = list(ds.query(query))
        count = result[0][0] if result else "?"
        log.info("  %s: %s", label, count)


def export_turtle_dumps(ds, output_dir):
    """Write hvd-tagged.ttl (full HVD graph) and hvd-valid.ttl (datasets with
    English title + description) into the output directory."""
    hvd_graph = ds.graph(GRAPH_HVD_DATASETS)
    if len(hvd_graph) == 0:
        log.warning("Skipping Turtle dumps — hvd_datasets graph is empty.")
        return

    log.info("Exporting Turtle dumps...")
    tagged_path = os.path.join(output_dir, "hvd-tagged.ttl")
    hvd_graph.serialize(destination=tagged_path, format="turtle")
    log.info("  hvd-tagged.ttl: %s bytes", f"{os.path.getsize(tagged_path):,}")

    query_valid = """
        PREFIX dcat:   <http://www.w3.org/ns/dcat#>
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX dcatap: <http://data.europa.eu/r5r/>
        PREFIX hvd-ir: <http://data.europa.eu/eli/reg_impl/2023/138/oj>
        CONSTRUCT { ?dataset ?p ?o }
        WHERE {
            GRAPH <%s> {
                ?dataset a dcat:Dataset ;
                         dcatap:applicableLegislation hvd-ir: ;
                         dcterms:title ?title ;
                         dcterms:description ?desc ;
                         ?p ?o .
            }
        }
    """ % GRAPH_HVD_DATASETS
    valid_graph = Graph()
    for triple in ds.query(query_valid):
        valid_graph.add(triple)
    valid_path = os.path.join(output_dir, "hvd-valid.ttl")
    valid_graph.serialize(destination=valid_path, format="turtle")
    log.info("  hvd-valid.ttl:  %s bytes", f"{os.path.getsize(valid_path):,}")


def build_graph(cache_path, endpoint, refresh, output_dir):
    """Return the local Dataset, either loaded from the N-Quads cache or
    freshly fetched from the endpoint (and then cached)."""
    # default_union=True: default graph = union of all named graphs (Virtuoso behaviour)
    ds = Dataset(default_union=True)

    if cache_path and os.path.exists(cache_path) and not refresh:
        log.info("Loading cached dataset from %s ...", cache_path)
        ds.parse(cache_path, format="nquads")
        log.info("Loaded %s triples from cache.", f"{len(ds):,}")
        return ds

    load_vocabularies(ds)
    failures = load_hvd_metadata(ds, endpoint)
    if failures:
        log.warning("%d fetch step(s) failed — the reporting CSVs may undercount:", len(failures))
        for f in failures:
            log.warning("  %s", f)

    validate(ds)
    export_turtle_dumps(ds, output_dir)

    if cache_path:
        log.info("Saving full Dataset to %s ...", cache_path)
        ds.serialize(destination=cache_path, format="nquads")
        log.info("  %s bytes", f"{os.path.getsize(cache_path):,}")

    return ds


# ---------------------------------------------------------------------------
# Phase B - reporting queries
# ---------------------------------------------------------------------------


def run_reporting_queries(ds, output_dir):
    """Run every reporting query locally and save one CSV per query.

    Returns the list of (filename, error) pairs for queries that failed.
    """
    hvd_triples = len(ds.graph(GRAPH_HVD_DATASETS))
    if hvd_triples == 0:
        log.error("hvd_datasets graph is empty — nothing to report on.")
        return [(filename, "empty graph") for filename, _ in ALL_QUERIES]

    log.info("hvd_datasets graph: %s triples — running %d queries locally.",
             f"{hvd_triples:,}", len(ALL_QUERIES))
    errors = []
    for filename, query in ALL_QUERIES:
        t0 = time.time()
        try:
            csv_bytes = results_to_csv(ds.query(query))
            _, row_count = save_csv(csv_bytes, output_dir, filename)
            log.info("  %s: %d rows (%.1fs)", filename, row_count, time.time() - t0)
        except Exception as e:
            log.error("  %s: ERROR (%.1fs): %s", filename, time.time() - t0, e)
            errors.append((filename, str(e)))
    return errors


def package_zip(output_dir, zip_path):
    log.info("Creating %s ...", zip_path)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(output_dir):
            for fname in sorted(files):
                fpath = os.path.join(root, fname)
                arcname = os.path.relpath(fpath, output_dir)
                zf.write(fpath, arcname)
                log.info("  added %s", arcname)
    log.info("Done — %s (%s bytes)", zip_path, f"{os.path.getsize(zip_path):,}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="ETL for the HVD Reporting application: builds a local RDF "
                    "graph from data.europa.eu and produces the reporting CSVs.")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR,
                        help="Directory for the CSV/Turtle outputs "
                             "(default: %(default)s)")
    parser.add_argument("--cache", default=DEFAULT_CACHE,
                        help="N-Quads file used to persist/reload the fetched graph "
                             "(default: %(default)s). Pass an empty string to disable.")
    parser.add_argument("--refresh", action="store_true",
                        help="Re-fetch everything even if the cache file exists.")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT,
                        help="Remote SPARQL endpoint (default: %(default)s)")
    parser.add_argument("--zip", metavar="PATH", nargs="?", const="hvdReport.zip",
                        default=None,
                        help="Also package the output directory as a zip "
                             "(default path if the flag is given alone: hvdReport.zip)")
    parser.add_argument("--app-data-dir", metavar="DIR", default=None,
                        help="Also write the compliance_<catalogue_id>.json files "
                             "directly into this directory (point it at the "
                             "dashboard/data folder of the HVD reporting app).")
    parser.add_argument("--skip-compliance", action="store_true",
                        help="Skip generating the per-catalogue compliance JSON files.")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Debug-level logging.")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s",
                        datefmt="%H:%M:%S")

    os.makedirs(args.output_dir, exist_ok=True)

    ds = build_graph(args.cache or None, args.endpoint, args.refresh,
                     args.output_dir)
    errors = run_reporting_queries(ds, args.output_dir)

    if not args.skip_compliance:
        try:
            write_compliance_files(ds, args.output_dir, args.app_data_dir)
        except Exception as e:
            log.error("Compliance JSON generation failed: %s", e)
            errors.append(("compliance JSON files", str(e)))

    if args.zip:
        package_zip(args.output_dir, args.zip)

    if errors:
        log.error("%d/%d queries failed:", len(errors), len(ALL_QUERIES))
        for filename, err in errors:
            log.error("  %s: %s", filename, err)
        return 1
    log.info("All %d reporting files written to %s", len(ALL_QUERIES),
             os.path.abspath(args.output_dir))
    return 0


if __name__ == "__main__":
    sys.exit(main())
