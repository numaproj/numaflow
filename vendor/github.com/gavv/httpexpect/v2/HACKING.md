# Hacking guidelines

<!-- toc -->

- [Working on a task](#working-on-a-task)
  * [Choosing a task](#choosing-a-task)
  * [Creating pull request](#creating-pull-request)
- [Developer instructions](#developer-instructions)
  * [Development dependencies](#development-dependencies)
  * [Makefile targets](#makefile-targets)
- [Code style](#code-style)
  * [Comment formatting](#comment-formatting)
- [Project internals](#project-internals)
  * [Object tree](#object-tree)
  * [Failure reporting](#failure-reporting)

<!-- tocstop -->

## Working on a task

### Choosing a task

Choosing a task is easy:

* Find a free task with **help wanted** or **good first issue** tag. The latter means that the task does not require deep knowldge of the project.

* **Leave a comment** in the task, indicating that you want to work on it. This allows to assign you to the task and to ensure that others wont work on it on the same time.

### Creating pull request

Please follow a few simple rules to ease the work of the reviewer:

* Add a **link to the task** in pull request description.

* Until pull request is ready to be merged, use GitHub **draft** feature.

* If you want pull request to be reviewed (no matter is it draft or not), use GitHub **request review** feature.

* When you submit changes after review, don't forget to **re-request review**.

* When you adderess issues raised during review, **don't resolve discussions by yourself**. Instead, leave a comment or thumbs up on that discussion.

## Developer instructions

### Development dependencies

For development, you need two additional dependencies:

* [golangci-lint](https://golangci-lint.run/usage/install/#local-installation)

* [stringer](https://github.com/golang/tools)

    `go install golang.org/x/tools/cmd/stringer@latest`

### Makefile targets

Re-generate, build, lint, and test everything:

```
make
```

Run tests:

```
make test
```

Run only short tests:

```
make short
```

Run gofmt:

```
make fmt
```

Run go generate:

```
make gen
```

Run go mod tidy:

```
make tidy
```

Generate TOC in HACKING.md:

```
make toc
```

## Code style

### Comment formatting

Exported functions should have documentation comments, formatted as follows:

* short function description, indented with one SPACE
* empty line
* optional details, indented with one SPACE
* empty line
* `Example:` line, indented with one SPACE
* empty line
* example code, indented with one TAB
* no more empty lines

**GOOD:**

```go
// Short function description.
//
// Optional details, probably multiple
// lines or paragraphs.
//
// Example:
//
//	exampleCode()
func MyFunction() { ... }
```

**BAD:** no space after `//`:

```go
//Short function description.
//
// Example:
//
//	exampleCode()
func MyFunction() { ... }
```

**BAD:** missing empty line before `Example:`

```go
// Short function description.
// Example:
//
//	exampleCode()
func MyFunction() { ... }
```

**BAD:** missing empty line after `Example:`

```go
// Short function description.
//
// Example:
//	exampleCode()
func MyFunction() { ... }
```

**BAD:** forgetting to indent example code:

```go
// Short function description.
//
// Example:
//
// exampleCode()
func MyFunction() { ... }
```

**BAD:** using spaces instead of TAB to indent example code:

```go
// Short function description.
//
// Example:
//
//  exampleCode()
func MyFunction() { ... }
```

**BAD:** extra empty line between comment and function:

```go
// Short function description.
//
// Example:
//
//	exampleCode()

func MyFunction() { ... }
```

## Project internals

### Object tree

The typical user workflow looks like this:

* create `Expect` instance (root object) using `httpexpect.Default` or `httpexpect.WithConfig`
* use `Expect` methods to create `Request` instance (HTTP request builder)
* use `Request` methods to configure HTTP request (e.g. `WithHeader`, `WithText`)
* use `Request.Expect()` method to send HTTP request and receive HTTP response; the method returns `Response` instance (HTTP response matcher)
* use `Response` methods to make assertions on HTTP response
* use `Response` methods to create child matcher objects for HTTP response payload (e.g. `Response.Headers()` or `Response.Body()`)
* use methods of matcher objects to make assertions on payload, or to create nested child matcher objects

All objects described above are linked into a tree using `chain` struct:

* every object has `chain` field
* when a child object is created (e.g. `Expect` creates `Request`, `Request` creates `Response`, and so on), the child object clones `chain` of its parent and stores it inside its `chain` field
* when an object performs an assertion (e.g. user calls `IsEqual`), it creates a temporary clone of its `chain` and uses it to report failure or success

`chain` maintains context needed to report succeeded or failed assertions:

* `AssertionContext` defines *where* the assertion happens: path to the assertion in object tree, pointer to current request and response, etc.
* `AssertionHandler` defines *what* to do in response to success or failure
* `AssertionSeverity` defines *how* to treat failures, either as fatal or non-fatal

These fields are inherited by child `chain` when it is cloned.

In addition, `chain` maintains a reference to its parent and flags indicating whether a failure happened on the `chain` or any of its children.

When success or failure is reported, the following happens:

* `chain` invokes `AssertionHandler` and passes `AssertionContext` and `AssertionFailure` to it
* in case of failure, `chain` raises a flag on itself, indicating failure
* in case of failure, `chain` raises a flag on its parents and garndparents (up to the tree root), indicating that their children have failures (this feature is rarely used)

These failure flags are then used to ignore all subsequent assertions on a failed branch of the object tree. For example, if you run this code:

```go
e.GET("/test").Expect().Status(http.StatusOK).Body().IsObject()
```

and if `Status()` assertion failed, then this branch of the tree will be marked as failed, and calls to `Body()` and `IsObject()` will be just ignored. This is achieved by inheriting failure flag when cloning `chain`, and checking this flag in every assertion.

### Failure reporting

`AssertionHandler` is an interface that is used to handle every succeeded or failed assertion (like `IsEqual`).

It can be implemented by user if the user needs very precise control on assertion handling. In most cases, however, user does not need it, and just uses `DefaultAssertionHandler` implementation, which does the following:

* pass `AssertionContext` and `AssertionFailure` to `Formatter`, to get formatted message
* when reporting failed assertion, pass formatted message to `Reporter`
* when reporting succeeded assertion, pass formatted message to `Logger`

`Formatter`, `Reporter`, and `Logger` are also interfaces that can be implemented by user. Again, in most cases user can use one of the available implementations:

* `DefaultFormatter` for `Formatter`
* `testing.T`, `FatalReporter`, `AssertReporter`, or `RequireReporter` for `Reporter`
* `testing.T` for `Logger`

In most cases, all the user needs to do is to select which reporter to use: `testing.T` or `FatalReporter` for non-fatal and fatal failure reports using standard `testing` package, and `AssertReporter` or `RequireReporter` for non-fatal and fatal failure reports using `testify` package (which adds nice backtrace and indentation).

For the rest, we will automatically employ default implementations (`DefaultFormatter`, `DefaultAssertionHandler`).

Note that `Formatter`, `Reporter`, and `Logger` are used only by `DefaultAssertionHandler`. If the user provides custom `AssertionHandler`, that implementation is free to ignore these three interfaces and can do whatever it wants.
