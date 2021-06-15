# kinesis-dms-record-generator

- [kinesis-dms-record-generator](#kinesis-dms-record-generator)
  * [Summary](#summary)
  * [The problem](#the-problem)
  * [The solution](#the-solution)
- [Dependencies](#dependencies)
- [Running as bin](#running-as-bin)
- [Pattern on filenames](#pattern-on-filenames)
- [Prompts](#prompts)
- [Running Tests](#running-tests)
- [Contributing](#contributing)


## Summary

> This is  simple CLI, to help developers simulate the records that Amazon DMS sends to
> a kinesis stream, when running Migration Tasks.

## The problem

> When developers are working with DMS + Kinesis, it's very difficult to simulate the stream locally,
> therefore, developer's productivity is impaired. This tool aims in improving the developer experience and productivity

## The solution

> By using tools such as [Data Grip](https://www.jetbrains.com/datagrip/), the developer can right-click the table,
> and export the table content to a JSON File. This tool will feed from this JSON file,
> and it will automatically generate the Kinesis Record, and use the aws-cli to put the records in the stream

# Dependencies

Before using this tool, you must have the following tools configured in your machine

- [aws cli](https://aws.amazon.com/cli/)
- [Localstack](https://github.com/localstack/localstack) (with kinesis enabled)

# Running as bin

```bash
npx dms-kinesis-gen
```

or install it globally with

```bash
npm i -g dms-kinesis-gen
```

And then use the command

```bash
dms-kinesis-gen
```

# Pattern on filenames

All the files inside the folder should follow the following pattern.

```bash
loadOrder.schema.table.json
```

For example

```bash
1.OT.CUSTOMER.json
2.OT.PRODUCTS.json
```

The bigger the load-order, first it will be loaded, following the same patter from [Amazon DMS Docs](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TableMapping.SelectionTransformation.Selections.html).

For the example files, the order will be:
```bash
2.OT.PRODUCTS.json
1.OT.CUSTOMER.json
```

The JSON can have a single record, or an array of records, for example:

```json
[
  {
    "ID": 1,
    "NAME": "Joselito Silva"
  }
]
```

This JSON file will generate the following Kinesis Record

```json
{
  "data": {
    "ID": 1,
    "NAME": "Joselito Silva"
  },
  "metadata": {
    "timestamp": "2021-02-21T23:02:36.5680Z",
    "record-type": "data",
    "operation": "load",
    "partition-key-type": "primary-key",
    "schema-name": "OT",
    "table-name": "CUSTOMER"
  }
}
```

# Prompts

```bash
√ What's the folder where the JSON files are located ? ... C:\Users\my-user\Documents\cdc-files
√ What's the name of the kinesis stream? ... my-localstack-stream
√ What's the partition key? ... 1
√ What's the localstack endpoint? ... http://localhost:4566
√ What's the name operation you want to simulate? » LOAD
```

The cli will load all files inside the **files** folder, and load them to the kinesis stream

# Running Tests

```bash
yarn test
```

# Running Lint

```bash
yarn lint

# or with fix

yarn lint:fix
```

# Contributing

Check the [contributing.md](./CONTRIBUTING.md) file for more information