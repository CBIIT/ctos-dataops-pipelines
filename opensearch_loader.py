#!/usr/bin/env python3
"""
Core OpenSearch loader logic (Neo4j -> OpenSearch).

Adapted from the legacy standalone loader. Refactored so behavior is callable
as `opensearch_loading(argList)` for use by the Prefect wrapper, while
preserving a CLI entry point for local runs.
"""

import argparse
import os
import re
from typing import Optional

import boto3
import yaml
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection
from opensearchpy.helpers import streaming_bulk
from neo4j import GraphDatabase

from bento.common.utils import get_logger, print_config
from os_loader_icdc_schema import (
    DESCRIPTION,
    ENUM,
    ICDC_Schema,
    PROP_ENUM,
    PROP_TYPE,
    PROPERTIES,
    REQUIRED,
)
from os_loader_icdc_props import Props

logger = get_logger("ESLoader")
OPENSEARCH_DATA = "opensearch_data"


class ESLoader:
    def __init__(
        self,
        es_host,
        neo4j_driver,
        aws_session: Optional[boto3.Session] = None,
        region: str = "us-east-1",
    ):
        self.neo4j_driver = neo4j_driver
        self.model = None
        timeout_seconds = 60
        if "amazonaws.com" in es_host:
            session = aws_session or boto3.Session()
            awsauth = AWSV4SignerAuth(session.get_credentials(), region, "es")
            self.es_client = OpenSearch(
                hosts=[es_host],
                http_auth=awsauth,
                use_ssl=True,
                verify_certs=True,
                connection_class=RequestsHttpConnection,
                timeout=timeout_seconds,
            )
        else:
            self.es_client = OpenSearch(hosts=[es_host], timeout=timeout_seconds)

    def create_index(self, index_name, mapping):
        """Create an index in OpenSearch (idempotent via ignore=400)."""
        return self.es_client.indices.create(
            index=index_name,
            body={
                "settings": {
                    "number_of_shards": 1,
                    "index.mapping.nested_objects.limit": 100000,
                },
                "mappings": {"properties": mapping},
            },
            ignore=400,
        )

    def delete_index(self, index_name):
        return self.es_client.indices.delete(index=index_name, ignore_unavailable=True)

    def get_data(
        self, cypher_query: str, fields, skip: int = 0, limit: int = 10_000_000
    ):
        """Yield one document per Neo4j record for streaming_bulk."""
        with self.neo4j_driver.session() as session:
            result = session.run(cypher_query, {"skip": skip, "limit": limit})
            for record in result:
                keys = list(record.keys())
                if len(keys) == 1 and keys[0].lower() == OPENSEARCH_DATA.lower():
                    record = record[keys[0]]
                doc = {}
                for key in fields:
                    doc[key] = record[key]
                yield doc

    def recreate_index(self, index_name, mapping):
        logger.info(f'Deleting old index: "{index_name}"')
        logger.info(self.delete_index(index_name))
        logger.info(f'Creating index: "{index_name}"')
        logger.info(self.create_index(index_name, mapping))

    def load(self, index_name, mapping, cypher_queries):
        self.recreate_index(index_name, mapping)
        logger.info("Indexing data from Neo4j")
        total_successes = 0
        total_documents = 0
        for i, cypher_query in enumerate(cypher_queries):
            query = cypher_query.get("query")
            if query is None:
                raise Exception(f"A query entry is missing for {index_name}")
            page_size = cypher_query.get("page_size") or 0
            logger.info(f"Executing index query {i + 1}/{len(cypher_queries)}")
            if page_size > 0:
                logger.info(f"Page size is set to {page_size}")
                skip = 0
                total = page_size
                while total == page_size:
                    successes, total = self.bulk_load(
                        index_name,
                        self.get_data(
                            query, mapping.keys(), skip=skip, limit=page_size
                        ),
                    )
                    total_successes += successes
                    total_documents += total
                    logger.info(
                        f"Indexing in progress: successfully indexed "
                        f"{total_successes}/{total_documents} documents"
                    )
                    skip += page_size
            else:
                logger.info("Pagination is disabled")
                successes, documents = self.bulk_load(
                    index_name, self.get_data(query, mapping.keys())
                )
                total_successes += successes
                total_documents += documents
        logger.info(
            f"Indexing completed: successfully indexed "
            f"{total_successes}/{total_documents} documents"
        )
        return total_successes

    def bulk_load(self, index_name, data):
        successes = 0
        total = 0
        for ok, _ in streaming_bulk(
            client=self.es_client,
            index=index_name,
            actions=data,
            max_retries=2,
            initial_backoff=10,
            max_backoff=20,
            max_chunk_bytes=10485760,
        ):
            total += 1
            successes += 1 if ok else 0
        return successes, total

    def load_about_page(self, index_name, mapping, file_name):
        logger.info("Indexing content from about page")
        if not os.path.isfile(file_name):
            raise Exception(f'"{file_name}" is not a file!')
        self.recreate_index(index_name, mapping)
        with open(file_name) as file_obj:
            about_file = yaml.safe_load(file_obj)
            for page in about_file:
                logger.info(f'Indexing about page "{page["page"]}"')
                self.index_data(index_name, page, f'page{page["page"]}')

    def read_model(self, model_files, prop_file):
        for file_name in model_files:
            if not os.path.isfile(file_name):
                raise Exception(f'"{file_name}" is not a file!')
        if not os.path.isfile(prop_file):
            raise Exception(f'"{prop_file}" is not a file!')
        self.model = ICDC_Schema(model_files, Props(prop_file))

    def load_model(self, index_name, mapping, subtype):
        logger.info("Indexing data model")
        if not self.model:
            logger.warning(
                f"Data model is not loaded, {index_name} will not be loaded!"
            )
            return 0
        self.recreate_index(index_name, mapping)
        successes, total = self.bulk_load(index_name, self.get_model_data(subtype))
        logger.info(f"Model indexing completed: {successes}/{total} documents")
        return successes

    def get_model_data(self, subtype):
        nodes = self.model.nodes
        for node_name, obj in nodes.items():
            props = obj[PROPERTIES]
            if subtype == "node":
                yield {
                    "type": "node",
                    "node": node_name,
                    "node_name": node_name,
                    "node_kw": node_name,
                }
            else:
                for prop_name, prop in props.items():
                    # Skip relationship-based properties
                    if "@relation" in obj[PROPERTIES][prop_name][PROP_TYPE]:
                        continue
                    if subtype == "property":
                        yield {
                            "type": "property",
                            "node": node_name,
                            "node_name": node_name,
                            "property": prop_name,
                            "property_name": prop_name,
                            "property_kw": prop_name,
                            "property_description": prop.get(DESCRIPTION, ""),
                            "property_required": prop.get(REQUIRED, False),
                            "property_type": (
                                PROP_ENUM if ENUM in prop else prop[PROP_TYPE]
                            ),
                        }
                    elif subtype == "value" and ENUM in prop:
                        for value in prop[ENUM]:
                            yield {
                                "type": "value",
                                "node": node_name,
                                "node_name": node_name,
                                "property": prop_name,
                                "property_name": prop_name,
                                "property_description": prop.get(DESCRIPTION, ""),
                                "property_required": prop.get(REQUIRED, False),
                                "property_type": PROP_ENUM,
                                "value": value,
                                "value_kw": value,
                            }

    def index_data(self, index_name, object, id):
        self.es_client.index(index=index_name, body=object, id=id)


def _validate_cypher_queries(cypher_queries):
    if type(cypher_queries) is not list:
        raise Exception('The required property "cypher_queries" must be a list')
    for i, cypher_query in enumerate(cypher_queries):
        if type(cypher_query) is not dict:
            raise Exception(
                'Each entry in the "cypher_queries" list must be a dict with a "query" property'
            )
        query = cypher_query.get("query")
        if query is None:
            raise Exception(
                'The required property "query" is missing from a "cypher_queries" entry'
            )
        page_size = cypher_query.get("page_size")
        if not _check_query_for_pagination(query):
            logger.warning(
                f'Pagination parameters are missing from "cypher_queries" entry {i + 1}, '
                f"pagination will be disabled for this query"
            )
            cypher_query["page_size"] = 0
        elif page_size is None:
            logger.warning(
                f'The page_size property is missing from "cypher_queries" entry {i + 1}, '
                f"pagination will be disabled for this query"
            )
            cypher_query["page_size"] = 0


def _check_query_for_pagination(query: str):
    return (
        re.search(r"skip\s*\$skip\s*limit\s*\$limit", query, re.IGNORECASE) is not None
    )


def opensearch_loader(argList: dict):
    """
    Load OpenSearch indices from Neo4j (and optionally about-page + model files).

    Expected keys in argList:
        indices_file  : path to the indices YAML
        es_host       : OpenSearch host (bare hostname for AWS domains)
        neo4j_uri     : bolt://host:port
        neo4j_user    : Neo4j username
        neo4j_password: Neo4j password
        region        : AWS region (default us-east-1)
        aws_session   : optional boto3.Session (for cross-account assume-role)
        about_file    : optional path (required only if indices YAML uses type=about_file)
        model_files   : optional list of paths (required only if indices YAML uses type=model)
        prop_file     : optional path (required only if indices YAML uses type=model)
        indices_filter: optional list of index names to load (default: all)
    """
    with open(argList["indices_file"]) as f:
        indices = yaml.safe_load(f)["Indices"]

    indices_filter = argList.get("indices_filter") or []
    if indices_filter:
        indices = [i for i in indices if i.get("index_name") in indices_filter]
        logger.info(f"Filtering to indices: {indices_filter}")

    neo4j_driver = GraphDatabase.driver(
        argList["neo4j_uri"],
        auth=(argList["neo4j_user"], argList["neo4j_password"]),
        encrypted=False,
    )

    loader = ESLoader(
        es_host=argList["es_host"],
        neo4j_driver=neo4j_driver,
        aws_session=argList.get("aws_session"),
        region=argList.get("region", "us-east-1"),
    )

    load_model = False
    if argList.get("model_files") and argList.get("prop_file"):
        loader.read_model(argList["model_files"], argList["prop_file"])
        load_model = True

    summary = {}
    for index in indices:
        index_name = index.get("index_name")
        summary[index_name] = "ERROR!"
        logger.info(f'Begin loading index: "{index_name}"')
        try:
            if "type" not in index or index["type"] == "neo4j":
                cypher_queries = index.get("cypher_queries")
                cypher_query = index.get("cypher_query")
                if cypher_queries is None and cypher_query is not None:
                    cypher_queries = [{"query": cypher_query}]
                _validate_cypher_queries(cypher_queries)
                summary[index_name] = loader.load(
                    index_name, index["mapping"], cypher_queries
                )
            elif index["type"] == "about_file":
                if argList.get("about_file"):
                    loader.load_about_page(
                        index_name, index["mapping"], argList["about_file"]
                    )
                    summary[index_name] = "Loaded Successfully"
                else:
                    logger.warning(
                        f'"about_file" not provided, {index_name} will not be loaded!'
                    )
            elif index["type"] == "model":
                if load_model and "subtype" in index:
                    loader.load_model(index_name, index["mapping"], index["subtype"])
                    summary[index_name] = "Loaded Successfully"
                else:
                    logger.warning(
                        f'"model_files"/"prop_file" not provided, '
                        f"{index_name} will not be loaded!"
                    )
            elif index["type"] == "external":
                logger.info(
                    "External data index created - loading will be done via data retriever service"
                )
                loader.create_index(index_name, index["mapping"])
                summary[index_name] = "Index created or already exists"
            else:
                logger.error(f'Unknown index type: "{index["type"]}"')
        except Exception as ex:
            logger.error(f'There is an error while loading "{index_name}"')
            logger.error(ex)

    logger.info("Index loading summary:")
    for index_name, status in summary.items():
        logger.info(f"{index_name}: {status}")

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Load data from Neo4j to OpenSearch/Elasticsearch"
    )
    parser.add_argument("indices_file", type=str, help="Path to indices YAML")
    parser.add_argument(
        "config_file", type=argparse.FileType("r"), help="Loader configuration YAML"
    )
    args = parser.parse_args()

    config = yaml.safe_load(args.config_file)["Config"]
    print_config(logger, config)

    argList = {
        "indices_file": args.indices_file,
        "es_host": config["es_host"],
        "neo4j_uri": config["neo4j_uri"],
        "neo4j_user": config["neo4j_user"],
        "neo4j_password": config["neo4j_password"],
        "about_file": config.get("about_file"),
        "model_files": config.get("model_files"),
        "prop_file": config.get("prop_file"),
        "region": config.get("region", "us-east-1"),
    }
    opensearch_loader(argList)


if __name__ == "__main__":
    main()
