# dataimport

General purpose library for data imports - CLI interface, multiple pipelines, configuration management.

Each pipeline requires a distinct configuration or a settings file that defines the classes for 
each **stage** of each **mode** and the relationship between them.

There are three distinct **modes**: **resolve**, **assemble** and **load**. The **resolve** mode has the
purpose of fetching and possibly manipulating the data. The **assemble** mode applies further and more
distinct data manipulation. The **load** mode prepares the data for delivery into its final recipient
system.

Each mode has a pipeline of sequential **stages**. **Resolve** has two stages: 1. **fetch**, that
acquires the data and 2: **analyse** that manipulates the fetched data. The input for **fetch** is
a **datasource**.

**Assemble** has the first **gather** stage that runs the **resolve** mode, it then analyses and
assembles, its inputs which are **products**.

**Load** is the final stage whereby **assemble** will run the previous stages. **Prepare** will prepare
for entry into a **target**, it's output. And **load** will deliver the data to its target.

    • Resolve
        ◦ Fetch
        ◦ Analyse
    • Assemble
        ◦ Gather
        ◦ Analyse
        ◦ Assemble
    • Load
        ◦ Assemble
        ◦ Prepare 
        ◦ Load

# Tests

Tests are defines in the `tests/` directory. This is a unit test that takes the csv file
`tests/origin.csv` that contains following data:

|a|b|c|
|-|-|-|
|1|2|3|
|4|5|6|
|8|9|10|

1. The resolve stage will keep only even numbers as `analysed.csv`:
```
2,4,6,8,10
```

2. The assemble stage will double the even numbers `mapping.json`:

```json
{"numbers": [[4, 8, 12, 16, 20]]}
```

3. The load stage will take the json data append to a DSpace item template and sava as `dspace.json`. 
A commented out version of the test is an integration test that uploads the json to the
[public sandbox instance](https://demo.dspace.org/) via REST API calls. You can run it by uncommenting the 
`TestDSpaceTarget` class in `test_load.py`.


To run unit tests do:

```bash
pytest
```

todo: usage guidelines