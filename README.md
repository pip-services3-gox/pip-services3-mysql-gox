# <img src="https://uploads-ssl.webflow.com/5ea5d3315186cf5ec60c3ee4/5edf1c94ce4c859f2b188094_logo.svg" alt="Pip.Services Logo" width="200"> <br/> MySQL components for Pip.Service in Golang

This module is a part of the [Pip.Services](http://pipservices.org) polyglot microservices toolkit.

The module contains the following packages:
 
- **Build** - a standard factory for constructing components
- **Connect** - instruments for configuring connections to the database.
- **Persistence** - abstract classes for working with the database that can be used for connecting to collections and performing basic CRUD operations

<a name="links"></a> Quick links:

* [Configuration](https://www.pipservices.org/recipies/configuration)
* [API Reference](https://godoc.org/github.com/pip-services3-gox/pip-services3-mysql-gox/)
* [Change Log](CHANGELOG.md)
* [Get Help](http://docs.pipservices.org/get_help/)
* [Contribute](http://docs.pipservices.org/contribute/)

## Use

Get the package from the Github repository:
```bash
go get -u github.com/pip-services3-gox/pip-services3-mysql-gox@latest
```

## Develop

For development you shall install the following prerequisites:
* Golang v1.18+
* Visual Studio Code or another IDE of your choice
* Docker
* Git

Run automated tests:
```bash
go test -v ./test/...
```

Generate API documentation:
```bash
./docgen.ps1
```

Before committing changes run dockerized test as:
```bash
./test.ps1
./clear.ps1
```

## Contacts

The library is created and maintained by **Sergey Seroukhov**, **Dmitrii Uzdemir** and **Dmitrii Levichev**.

The documentation is written by **Mark Makarychev**.