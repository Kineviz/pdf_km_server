# Plan for adding semantic search to the knowledge map

## Requirements

- Vectorize the `text` property of `Observation` table using `all-MiniLM-L6-v2` model and store the vectors in a separate table
- Add a new tab to the UI to test semantic search, which accepts a Kuzu zip file and a search query
- Do semantic search on the `text` property of `Observation` table by first querying the `ObservationTextVector` table + `ObservationTextVector_index` index and then returning the connected `Observation` nodes

## Schema Changes

- Create a new node table `ObservationTextVector` with `id STRING PRIMARY KEY` and `vector FLOAT[384]` properties, where `id` is the `id` of the `Observation` node
- Add vector index `ObservationTextVector_index`
- Add rel table `OBSERVATION_TEXT_VECTOR` from `Observation` to `ObservationTextVector`

## Links

- https://docs.kuzudb.com/extensions/vector/

## Example: creating embeddings and storing them in the database

```python
# pip install sentence-transformers
import kuzu
import os

db = kuzu.Database("example.kuzu")
conn = kuzu.Connection(db)

from sentence_transformers import SentenceTransformer
model = SentenceTransformer("all-MiniLM-L6-v2")

conn.execute("INSTALL vector; LOAD vector;")

conn.execute("CREATE NODE TABLE Book(id SERIAL PRIMARY KEY, title STRING, title_embedding FLOAT[384], published_year INT64);")
conn.execute("CREATE NODE TABLE Publisher(name STRING PRIMARY KEY);")
conn.execute("CREATE REL TABLE PublishedBy(FROM Book TO Publisher);")

titles = [
    "The Quantum World",
    "Chronicles of the Universe",
    "Learning Machines",
    "Echoes of the Past",
    "The Dragon's Call"
]
publishers = ["Harvard University Press", "Independent Publisher", "Pearson", "McGraw-Hill Ryerson", "O'Reilly"]
published_years = [2004, 2022, 2019, 2010, 2015]

for title, published_year in zip(titles, published_years):
    embeddings = model.encode(title).tolist()
    conn.execute(
        """
        CREATE (b:Book {
            title: $title,
            title_embedding: $embeddings,
            published_year: $year
        });""",
        {"title": title, "year": published_year, "embeddings": embeddings}
    )

    print(f"Inserted book: {title}")

for publisher in publishers:
    conn.execute(
        """CREATE (p:Publisher {name: $publisher});""",
        {"publisher": publisher}
    )
    print(f"Inserted publisher: {publisher}")

for title, publisher in zip(titles, publishers):
    conn.execute("""
        MATCH (b:Book {title: $title})
        MATCH (p:Publisher {name: $publisher})
        CREATE (b)-[:PublishedBy]->(p);
        """,
        {"title": title, "publisher": publisher}
    )
    print(f"Created relationship between {title} and {publisher}")
```

## Example: creating a vector index

```cypher
CALL CREATE_VECTOR_INDEX(
    'Book',
    'book_title_index',
    'title_embedding',
    metric := 'l2'
);
```

## Example: querying the vector index

```python
import kuzu

# Initialize the database
db = kuzu.Database("example.kuzu")
conn = kuzu.Connection(db)

# Install and load vector extension once again
conn.execute("INSTALL VECTOR;")
conn.execute("LOAD VECTOR;")

from sentence_transformers import SentenceTransformer
# Load a pre-trained embedding generation model
# https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2
model = SentenceTransformer("all-MiniLM-L6-v2")

query_vector = model.encode("quantum machine learning").tolist()
result = conn.execute(
    """
    CALL QUERY_VECTOR_INDEX(
        'Book',
        'book_title_index',
        $query_vector,
        $limit,
        efs := 500
    )
    RETURN node.title
    ORDER BY distance;
    """,
    {"query_vector": query_vector, "limit": 2})

print(result.get_as_pl())
```

## Example: querying the vector index and returning connected nodes

```python
result = conn.execute(
    """
    CALL QUERY_VECTOR_INDEX('Book', 'book_title_index', $query_vector, 2)
    WITH node AS n, distance
    MATCH (n)-[:PublishedBy]->(p:Publisher)
    RETURN p.name AS publisher, n.title AS book, distance
    ORDER BY distance
    LIMIT 5;
    """,
    {"query_vector": query_vector})
print(result.get_as_pl())
```